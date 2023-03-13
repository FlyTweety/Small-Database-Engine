#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  /*
  首先直接判断是否能够存储
  然后从第一页开始搜索，直接调用InsertTuple，如果insert失败，找下一�?
  */
  if(row.GetSerializedSize(schema_)>PAGE_SIZE){
    return false;
  }
  //get the first page
  TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),false);
  //use loop to find the nearest page which can insert the turple
  while(!NowPage->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    page_id_t NextPageId = NowPage->GetNextPageId();
    if (NextPageId == INVALID_PAGE_ID){
      int new_page_id = INVALID_PAGE_ID;
      //set as dirty page
      buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);
      TablePage *New_Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      //uncover
      buffer_pool_manager_->UnpinPage(New_Page->GetPageId(),false);
      if (New_Page==nullptr){
        return false;
      }
      //link the page
      New_Page->Init(new_page_id,NowPage->GetPageId(),log_manager_,txn);
      New_Page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      buffer_pool_manager_->UnpinPage(New_Page->GetPageId(),true);
      NowPage->SetNextPageId(new_page_id);
      break;
    }
    NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
    buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),false);
  }
  //set as dirty page
  buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  //Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  //we can find
  //save the old row
  Row old_row(rid);
  if(page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_) == 0){
    //cout << page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_) << endl;
    row.SetRowId(rid);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    ASSERT(row.GetRowId().GetPageId()!=INVALID_PAGE_ID, "Fail to update.Invalid page.");
    return true;
  }
  else{
    uint32_t slot_num = old_row.GetRowId().GetSlotNum();
    uint32_t tuple_size = page->GetTupleSize(slot_num);
    if(slot_num>=page->GetTupleCount() || TablePage::IsDeleted(tuple_size)){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }else{
      //space is not enough
      ASSERT(row.GetRowId().GetPageId()!=INVALID_PAGE_ID, "Invalid page.");
      //judge insert
      if(!InsertTuple(row,txn)){
        cout << "The turple is too large to update" << endl;
        return false;
      }
      //insert success, delete the old row
      page->WLatch();
      MarkDelete(rid,txn);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
  }
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page!=nullptr);
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  //get the first page
  TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if(NowPage == nullptr){
    return;
  }
  while(NowPage->GetNextPageId()!=INVALID_PAGE_ID){//get the nearest page which is valid
  page_id_t NextPageId = NowPage->GetNextPageId();//find next page
    buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//set as dirty page
    buffer_pool_manager_->DeletePage(NowPage->GetPageId());//delete this page
    NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
  }
  return;
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr){
    return false;
  }else{
    bool isGet;
    isGet=page->GetTuple(row,schema_,txn,lock_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return isGet;
  }
  //return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //get the first record
  //return TableIterator();
  RowId FirstId;
  auto page_id=first_page_id_;
  while(page_id!=INVALID_PAGE_ID){
    auto page=static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    auto isFound=page->GetFirstTupleRid(&FirstId);
    buffer_pool_manager_->UnpinPage(page_id,false);
    if(isFound){
      break;
    }
    page_id=page->GetNextPageId();
  }
  return TableIterator(this,FirstId);
}

TableIterator TableHeap::End() {
  //return TableIterator();
  return TableIterator(this,RowId(INVALID_ROWID));
}