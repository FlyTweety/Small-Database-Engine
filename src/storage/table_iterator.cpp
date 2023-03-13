#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"


TableIterator::TableIterator() {

}


TableIterator::TableIterator(TableHeap *_table_heap, RowId _rid)
    : table_heap(_table_heap),row(new Row(_rid)){
      //Row row_(_rid);
  if (row->GetRowId().GetPageId() != INVALID_PAGE_ID) {
    table_heap->GetTuple(row,nullptr);
    //row=&row_;
  }
}

TableIterator::TableIterator(const TableIterator &other):
table_heap(other.table_heap),row(new Row(*other.row))
{
  
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row->GetRowId()==itr.row->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
  return *(this->row);
}

TableIterator& TableIterator::operator=(const TableIterator &other){
  table_heap=other.table_heap;
  row=other.row;
  return *this;
}

Row *TableIterator::operator->() {
  ASSERT(*this != table_heap->End(),"itr is at end");
  return row;
}

TableIterator &TableIterator::operator++() {
  
  BufferPoolManager *buffer_pool_manager=table_heap->buffer_pool_manager_;//获得缓冲池管理权限
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row->GetRowId().GetPageId())); 
  ASSERT(page!=nullptr,"Can't have empty page!");
  //获得当前record所在页
  RowId NextId;//得到下一条record的rid
  bool isGet;
  isGet=page->GetNextTupleRid(row->GetRowId(),&NextId);
  //Row Nextrow(NextId);
  if(!isGet){//找不到下一条记录
    //当前记录为一页的最后一条记录S
    while(page->GetNextPageId()!=INVALID_PAGE_ID){
      auto next_page=static_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
      buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);//写回
      page=next_page;
      if(page->GetFirstTupleRid(&NextId)){
        break;//找到next了
      }
    }
  }
  delete row;//删掉旧的
  row= new Row(NextId);//新建一个新的，否则会一直累加
  if(*this!=table_heap->End()){//不是末尾
    Transaction *txn=nullptr;
    table_heap->GetTuple(row,txn);
  }

  buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);
  //没有修改，不是脏页
  return *this;
  
 //return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator old_heap(*this);
  ++(*this);
  return old_heap;
  //return TableIterator();
}
// #include "common/macros.h"
// #include "storage/table_iterator.h"
// #include "storage/table_heap.h"

// TableIterator::TableIterator() {
//   current_page_id = 0;
//   current_slot_id = 0;
//   table_heap = nullptr;
//   txn_ = nullptr;
// }

// TableIterator::TableIterator(const TableIterator &other) {
//   this->current_page_id = other.current_page_id;
//   this->current_slot_id = other.current_slot_id;
//   this->table_heap = other.table_heap;
//   this->txn_ = other.txn_;
// }

// TableIterator::TableIterator(page_id_t p, u_int32_t s, TableHeap *H, Transaction *txn){
//   current_page_id = p;
//   current_slot_id = s;
//   table_heap = H;
//   txn_ = txn;
// }
// TableIterator::~TableIterator() {

// }

// bool TableIterator::operator==(const TableIterator &itr) const {
//   return((itr.current_page_id == this->current_page_id)&&(itr.current_slot_id == this->current_slot_id));
// }

// bool TableIterator::operator!=(const TableIterator &itr) const {
//   return !((*this) == itr);
// }

// const Row &TableIterator::operator*() {
//   //ASSERT(false, "Not implemented yet.");
//   auto page = reinterpret_cast<TablePage*>(this->table_heap->buffer_pool_manager_->FetchPage(this->current_page_id));
//   RowId current_tuple_id;
//   ASSERT(page->GetFirstTupleRid(&current_tuple_id),"No valid tuple in this page.");

//   for(size_t i=0;i<this->current_slot_id;i++){
//     RowId next_rid;
//     page->GetNextTupleRid(current_tuple_id,&next_rid);
//     // ASSERT(page->GetNextTupleRid(current_tuple_id,&next_rid),"No valid tuple in this page.");
//     current_tuple_id = next_rid;
//   }
//   Row* new_row = new Row(current_tuple_id);
//   return *new_row;
// }

// Row *TableIterator::operator->() {
//   auto page = reinterpret_cast<TablePage*>(this->table_heap->buffer_pool_manager_->FetchPage(this->current_page_id));
//   RowId current_tuple_id;
//   ASSERT(page->GetFirstTupleRid(&current_tuple_id),"No valid tuple in this page.");
//   for(size_t i=0;i<this->current_slot_id;i++){
//     RowId next_rid;
//     ASSERT(page->GetNextTupleRid(current_tuple_id,&next_rid),"No valid tuple in this page.");
//     current_tuple_id = next_rid;
//   }
//   return new Row(current_tuple_id);
// }

// TableIterator &TableIterator::operator++() {
//   TablePage *page = reinterpret_cast<TablePage *>(this->table_heap->buffer_pool_manager_->FetchPage(this->current_page_id));
//   if(this->current_slot_id == page->GetSlotCount()){
//     this->current_slot_id = 0;
//     page_id_t next_page = page->GetNextPageId();
//     if(next_page == INVALID_PAGE_ID){
//       this->current_page_id = INVALID_PAGE_ID;
//       return *this;
//     }
//     //这里有些不太理解，当一页中的记录扫描完后，是跳转到下一页吗
//     else{
//       this->current_page_id = next_page;
//     }
//   }
//   else{
//     this->current_slot_id++;
//   }
//   return *this;
//   //if(this->current_slot_id == page->Get)
// }

// TableIterator TableIterator::operator++(int) {
//   TableIterator itr = *this;
//   ++(*this);
//   return itr;
// }
