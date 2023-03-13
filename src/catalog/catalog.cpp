#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  //先存map的长度，再存map的元�?
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf+offset, CATALOG_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  uint32_t len = table_meta_pages_.size();
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  MACH_WRITE_TO(uint32_t, buf+offset, len);
  offset += sizeof(uint32_t);
  for(auto it : table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) continue;
    MACH_WRITE_TO(uint32_t, buf+offset, it.first);
    offset += sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buf+offset, it.second);
    offset += sizeof(uint32_t);
  }
  len = index_meta_pages_.size();
  for(auto it:index_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  MACH_WRITE_TO(uint32_t, buf+offset, len);
  offset += sizeof(uint32_t);
  for(auto it : index_meta_pages_){
    if(it.second == INVALID_PAGE_ID) continue;
    MACH_WRITE_TO(uint32_t, buf+offset, it.first);
    offset += sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buf+offset, it.second);
    offset += sizeof(uint32_t);
  }
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t table_len = table_meta_pages_.size();
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) table_len--;
  }
  uint32_t index_len = index_meta_pages_.size();
  for(auto it:index_meta_pages_){
    if(it.second == INVALID_PAGE_ID) index_len--;
  }
  return table_len *(sizeof(table_id_t) + sizeof(page_id_t)) + index_len *(sizeof(index_id_t) + sizeof(page_id_t)) 
            +3*sizeof(uint32_t);
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  CatalogMeta* meta = new(heap->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_FROM(uint32_t, buf+offset);
  offset = offset + sizeof(uint32_t);
  if (magic_num != CATALOG_METADATA_MAGIC_NUM) {
    ASSERT(false, "error");
    return nullptr;
  }
  uint32_t len = MACH_READ_FROM(uint32_t, buf+offset);
  offset = offset + sizeof(uint32_t);
  for(u_int32_t i=0; i<len; i++){
    table_id_t first_table = MACH_READ_FROM(table_id_t, buf+offset);
    offset = offset + sizeof(table_id_t);
    page_id_t second_table = MACH_READ_FROM(page_id_t, buf+offset);
    offset = offset + sizeof(page_id_t);
    meta->table_meta_pages_.insert(make_pair(first_table,second_table));
  }
  meta->table_meta_pages_.insert({len, INVALID_PAGE_ID});
  len = MACH_READ_FROM(uint32_t, buf+offset);
  offset = offset + sizeof(uint32_t);
  for(u_int32_t i=0; i<len; i++){
    index_id_t first_index = MACH_READ_FROM(index_id_t, buf+offset);
    offset = offset + sizeof(table_id_t);
    page_id_t second_index = MACH_READ_FROM(page_id_t, buf+offset);
    offset = offset + sizeof(page_id_t);
    meta->index_meta_pages_.insert(make_pair(first_index,second_index));
  }
  meta->index_meta_pages_.insert({len, INVALID_PAGE_ID});

  return meta;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if (init == true) {
    catalog_meta_ = new(heap_->Allocate(sizeof(CatalogMeta)))CatalogMeta;
    // Page* page;
    // page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    // catalog_meta_->SerializeTo(page->GetData());
    // buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
  }
  else {
    Page* page;
    page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData(), heap_);
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();

    for (auto it:catalog_meta_->table_meta_pages_) {
      //cout <<it.first <<" "<< it.second << endl;
      if(it.second == INVALID_PAGE_ID) continue;
      LoadTable(it.first, it.second);
    }
    for (auto it:catalog_meta_->index_meta_pages_) {
      //cout <<it.first << " "<<it.second<<endl;
      if(it.second == INVALID_PAGE_ID) continue;
      LoadIndex(it.first, it.second);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  
  for(auto it : table_names_){
    if (it.first == table_name) {
      table_info = tables_[it.second];
      return DB_TABLE_ALREADY_EXIST;
    }
  }
  table_id_t table_id = next_table_id_++;
  table_info = TableInfo::Create(heap_);
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  TableMetadata *table_meta = table_meta->Create(table_id, table_name, page_id, schema, heap_);
  table_info->Init(table_meta, table_heap);

  catalog_meta_->table_meta_pages_.insert({table_id ,page_id});
  catalog_meta_->table_meta_pages_.insert({next_table_id_, INVALID_PAGE_ID});
  index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});
  table_names_.insert({table_name, table_id});
  tables_.insert({table_id, table_info});

  uint32_t len = table_meta->GetSerializedSize();
  char table_buff[len+1];
  table_meta->SerializeTo(table_buff);
  memcpy(page->GetData(), table_buff, len);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->FlushPage(page_id);

  len = catalog_meta_->GetSerializedSize();
  char cata_buff[len+1];
  catalog_meta_->SerializeTo(cata_buff);
  page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  memset(page->GetData(), 0, PAGE_SIZE);
  memcpy(page->GetData(), cata_buff, len);
  if(table_meta!=nullptr && table_heap!=nullptr)
  {
    //cout <<"create table success, " <<"table_id=" << table_id <<" "<<"page_id="<<page_id<<" "<<"next_table_id="<<next_table_id_<<endl;
    return DB_SUCCESS;
  }
  return DB_FAILED;
}


dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  for(auto it : table_names_){
    if (it.first == table_name) {
      table_info = tables_[it.second];
      return DB_SUCCESS;
    }
  }
  return DB_TABLE_NOT_EXIST;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto iter : table_names_){
    auto i = tables_.find(iter.second);
    tables.push_back(i->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  
  auto iter_T = table_names_.find(table_name);
  if(iter_T == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto it = index_names_[table_name];
  iter_T = it.find(index_name);
  if(iter_T != table_names_.end()){
    return DB_INDEX_ALREADY_EXIST; 
  } 

  index_id_t index_id = next_index_id_++;
  auto table_id = table_names_[table_name];
  index_info = IndexInfo::Create(heap_);
  IndexMetadata *index_meta;
  TableInfo* table_info = tables_[table_id];

  ;
  auto schema = table_info->GetSchema();
  vector<Column *> cols= schema->GetColumns();
  std::vector<uint32_t> key_map;
  for(size_t i=0; i<index_keys.size(); i++){
      auto col = index_keys[i];
      uint32_t col_index;
      dberr_t col_not_exist = table_info->GetSchema()->GetColumnIndex(col, col_index);
      if(col_not_exist) return DB_COLUMN_NAME_NOT_EXIST;
      key_map.push_back(col_index);
  }
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap_);
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  catalog_meta_->index_meta_pages_[index_id] = page_id;
  catalog_meta_->index_meta_pages_[next_index_id_] = INVALID_PAGE_ID;
  index_names_[table_name][index_name] = index_id;
  indexes_.insert({index_id, index_info});

  uint32_t len = index_meta->GetSerializedSize();
  char index_buff[len+1];
  index_meta->SerializeTo(index_buff);
  memcpy(page->GetData(), index_buff, len);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->FlushPage(page_id);

  len = catalog_meta_->GetSerializedSize();
  char cata_buff[len+1];
  catalog_meta_->SerializeTo(cata_buff);
  page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  memset(page->GetData(), 0, PAGE_SIZE);
  memcpy(page->GetData(), cata_buff, len);
  //cout << "create index success, "<<"index_id=" << index_id <<" "<<"page_id="<<page_id<<" "<<"next_index_id="<<next_index_id_<<endl;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto table_exist = index_names_.find(table_name);
  if (table_exist == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }

  for(auto it_T : index_names_){
    if(it_T.first == table_name){
      auto IDX = it_T.second;
      auto it = IDX.find(index_name);
      auto i  = indexes_.find(it->second);
      index_info = i->second;
      return DB_SUCCESS;
    }
  }
  return DB_INDEX_NOT_FOUND;
  
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  for(auto it_T : index_names_){
    if(it_T.first == table_name){
      auto IDX = it_T.second;
      for(auto it_I : IDX){
        auto i = indexes_.find(it_I.second);
        indexes.push_back(i->second);
      }
      return DB_SUCCESS;
    }
  }
  return DB_TABLE_NOT_EXIST;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  std::unordered_map<std::string, table_id_t>::iterator iter;
  iter = table_names_.find(table_name);
  if(iter == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t Tid = iter->second;
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::iterator iter2;
  iter2 = index_names_.find(table_name);
  for(auto it : iter2->second){
    DropIndex(table_name, it.first);
  }
  
  index_names_.erase(table_name);
  tables_.erase(Tid);
  page_id_t page_id = catalog_meta_->table_meta_pages_[Tid];
  catalog_meta_->table_meta_pages_.erase(Tid);
  buffer_pool_manager_->DeletePage(page_id);
  table_names_.erase(table_name);

  uint32_t len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  memset(page->GetData(), 0, PAGE_SIZE);
  memcpy(page->GetData(), meta, len);

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  //先找table，再找index
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::iterator iter;
  iter = index_names_.find(table_name);
  if(iter == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string,index_id_t>::iterator iter2;
  iter2 = iter->second.find(index_name);
  if(iter2 == iter->second.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t Iid = iter2->second;
  page_id_t page_id = catalog_meta_->index_meta_pages_[Iid];
  catalog_meta_->index_meta_pages_.erase(Iid);
  iter->second.erase(index_name);
  indexes_.erase(Iid);
  buffer_pool_manager_->DeletePage(page_id);

  uint32_t len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  memset(page->GetData(), 0, PAGE_SIZE);
  memcpy(page->GetData(), meta, len);

  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  
  Page* page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  return DB_SUCCESS;
  
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *pag = buffer_pool_manager_->FetchPage(page_id);
  char *dat = reinterpret_cast<char *> (pag->GetData());
  TableMetadata *table_meta;
  table_meta->DeserializeFrom(dat, table_meta, heap_);
  TableInfo *table_info = TableInfo::Create(heap_);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetSchema(), nullptr, nullptr, nullptr, heap_);
  table_info->Init(table_meta, table_heap);
  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;
  index_names_.insert({table_meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page* pag = buffer_pool_manager_->FetchPage(page_id);
  char *dat = reinterpret_cast<char *> (pag->GetData());
  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(dat, index_meta, heap_);
  TableInfo* tinfo = tables_[index_meta->GetTableId()];
  auto index_info = IndexInfo::Create(heap_);
  index_info->Init(index_meta, tinfo, buffer_pool_manager_);
  index_names_[tinfo->GetTableName()][index_meta->GetIndexName()] = index_meta->GetIndexId();
  indexes_[index_id] = index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

