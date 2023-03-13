#include "executor/execute_engine.h"
#include "glog/logging.h"
//#include <iostream> //linshijiade

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      // std::cout << "jinru createtable" << std::endl;
      // std::cout << "aaaaaaaaa" << std::endl;
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  string db_name = ast->child_->val_;
  if (dbs_.count(db_name) != 0) {
    cout << "Can't create database '" << db_name << "'; database exists" << endl;
    return DB_FAILED;
  }
  DBStorageEngine *new_db = new DBStorageEngine(db_name);
  dbs_.emplace(db_name, new_db);
  end = clock();
  cout << "Query OK, 1 row affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
  
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  string db_name = ast->child_->val_;
  if (dbs_.count(db_name) == 0) {
    cout << "Can't drop database '" << db_name << "'; database doesn't exist" << endl;
    return DB_FAILED;
  }
  dbs_.erase(db_name);
  end = clock();
  cout << "Query OK, 0 rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;

}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  clock_t begin, end;
  long unsigned int length = 8;
  long unsigned int i;
  begin = clock();
  if(dbs_.size()!=0){
    for(auto it = dbs_.begin(); it != dbs_.end(); it++){
      if(it->first.length()>length) length = it->first.length();
    }
  }
  
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  cout << "| Database";
  for(i=0;i<length-9+2;i++) cout << " ";
  cout << "|" << endl;
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  if(dbs_.size()!=0){
    
    for(auto it = dbs_.begin(); it != dbs_.end(); it++){
    cout << "| " << it->first;
    for(i=0;i<length-1-it->first.length()+2;i++) cout << " ";
    cout << "|" << endl;
  }
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  }
  
  end = clock();
  cout << dbs_.size() << " rows in set ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  string db_name = ast->child_->val_;
  if (dbs_.count(db_name) == 0) {
    cout << "Can't use database '" << db_name << "'; database doesn't exist" << endl;
    return DB_FAILED;
  }
  current_db_ = db_name;
  end = clock();
  cout << "Database changed ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  long unsigned int i = 0;
  long unsigned int length = 0;
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  std::vector<TableInfo *> tables;
  catalog->GetTables(tables);
  if(tables.empty()){
    cout << "Empty set" << endl;
    return DB_FAILED;
  }
  length = 10 + current_db_.length();
  std::vector<std::string> tables_name;
  for(auto it = tables.begin(); it != tables.end(); it++){
    tables_name.emplace_back((*it)->GetTableName());
    if((*it)->GetTableName().length()>length) length = (*it)->GetTableName().length();
  }
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  cout << "| Tables_in_" << current_db_;
  for(i=0;i<length+1-10-current_db_.length();i++) cout << " ";
  cout << "|" << endl;
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  for(i=0;i<tables_name.size();i++){
    cout << "| " << tables_name[i];
    for(long unsigned int j=0;j<length+1-tables_name[i].length();j++)
      cout << " ";
    cout << "|" << endl;
  }
  cout << "+";
  for(i=0;i<length+2;i++) cout << "-";
  cout << "+" << endl;
  end = clock();
  cout << tables_name.size() << " rows in set ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  // std::cout << "daoda "<<std::endl;
  //return DB_FAILED;
#ifdef ENABLE_EXECUTE_DEBUG
 LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // std::cout << "daoda "<<std::endl;
  clock_t begin, end;
  
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  TableInfo* temp = nullptr;
  string table_name = ast->child_->val_;
  ast = ast->child_;
  ast = ast->next_;//point to kNodeColumnDefinitionList
  std::vector<Column *> columns;
  pSyntaxNode primary_able = ast;//find primary keys
  std::vector<std::string> primary_key;
  primary_able = primary_able->child_;
  //cout << "haha";
  while(primary_able->next_!=nullptr) primary_able = primary_able->next_;
  //cout << "haha";
  //std::string compare1(primary_able->val_);
  if(primary_able->type_ == kNodeColumnList){
    primary_able = primary_able->child_;
    primary_key.push_back(primary_able->val_);
    while(primary_able->next_!=nullptr){
      primary_able = primary_able->next_;
      primary_key.push_back(primary_able->val_);
    }
  }
  ast = ast->child_;//point to first column_definition
  long unsigned int column_num = 0;
  //cout << "haha" ;
  // cout << "Primary number is " << primary_key.size() << endl;
  if(!primary_key.empty()){
    do{
      bool unique = false;
      bool nullable = true;
      std::string column_name;
      TypeId type;
      //std::string compare2(ast->val_);
      if(/*compare2=="unique"*//*strcmp(ast->val_, "unique")==0*/ast->val_!=NULL) unique = true;
      column_name = ast->child_->val_;
      for(long unsigned int i = 0; i < primary_key.size(); i++){
        if(primary_key[i]==column_name){
          unique = true;
          nullable = false;
          break;
        }
      }
      //std::string type_check;
      //type_check = ast->child_->next_->val_;
      if(strcmp(ast->child_->next_->val_, "int")==0){
        type = kTypeInt;
        Column p(column_name,type,column_num,nullable,unique);
        Column *new_column = new Column(p);
        // new_column = &p;
        columns.push_back(new_column);
      } 
      else if(strcmp(ast->child_->next_->val_, "float")==0){
        type = kTypeFloat;
        Column p(column_name,type,column_num,nullable,unique);
        Column *new_column = new Column(p);
        columns.push_back(new_column);
      } 
      else if(strcmp(ast->child_->next_->val_, "char")==0){
        char* value_check = new char;
        strcpy(value_check, ast->child_->next_->child_->val_);
        //value_check = ast->child_->next_->child_->val_;
        type = kTypeChar;
        long unsigned int MAX = 0;
        
        for(int i = 0; value_check[i] != '\0'; i++){
          if(value_check[i]=='-'||value_check[i]=='.'){
            cout << "You have an error in your SQL syntax." << endl;
            return DB_FAILED;
          }
          MAX = MAX * 10 + value_check[i] - '0';
        }
        Column p(column_name, type, MAX, column_num, nullable, unique);
        Column *new_column = new Column(p);
        columns.push_back(new_column);
      }
      column_num++;
      //std::string compare3(ast->next_->val_);
      if(/*compare3=="primary keys"*//*strcmp(ast->next_->val_, "primary keys")==0*/ast->next_->type_==kNodeColumnList) break;
      ast = ast->next_;
    }while(1);
  }
  else{
    do{
      bool unique = false;
      bool nullable = true;
      std::string column_name;
      TypeId type;
      //std::string compare4(ast->val_);
      if(/*compare4=="unique"*//*strcmp(ast->val_, "unique")==0*/ ast->val_!=NULL) unique = true;
      column_name = ast->child_->val_;
      
      // std::string type_check;
      // type_check = ast->child_->next_->val_;
      if(/*type_check=="int"*/strcmp(ast->child_->next_->val_, "int")==0){
        type = kTypeInt;
        Column p(column_name,type,column_num,nullable,unique);
        Column *new_column = new Column(p);
        columns.push_back(new_column);
      } 
      else if(strcmp(ast->child_->next_->val_, "float")==0){
        type = kTypeFloat;
        Column p(column_name,type,column_num,nullable,unique);
        Column *new_column = new Column(p);
        columns.push_back(new_column);
      } 
      else if(strcmp(ast->child_->next_->val_, "char")==0){
        char* value_check = new char;
        strcpy(value_check, ast->child_->next_->child_->val_);
        type = kTypeChar;
        long unsigned int MAX = 0;
        //const char* char_check = type_check.data();
        for(int i = 0; value_check[i] != '\0'; i++){
          if(value_check[i]=='-'||value_check[i]=='.'){
            cout << "You have an error in your SQL syntax." << endl;
            return DB_FAILED;
          }
          MAX = MAX * 10 + value_check[i] - '0';
        }
        Column p(column_name, type, MAX, column_num, nullable, unique);
        Column *new_column = new Column(p);
        columns.push_back(new_column);
      }
      column_num++;
      if(ast->next_==nullptr) break;
      ast = ast->next_;
    }while(1);
  }
  TableSchema new_schema(columns);
  TableSchema* schema = new TableSchema(new_schema);
  dberr_t flag = catalog->CreateTable(table_name, schema, nullptr, temp);
  if(flag==DB_TABLE_ALREADY_EXIST){
    cout << "Can't create table '" << table_name << "'; table already exists." << endl;
    return DB_FAILED;
  }
  //catalog->CreateTable(table_name, schema, nullptr, temp);
  end = clock();
  cout << "Query OK, 0 rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  TableInfo *temp;
  if(catalog->CreateTable(ast->child_->val_,NULL,nullptr,temp)!=DB_TABLE_ALREADY_EXIST){
    cout << "Can't drop table '" << ast->child_->val_ << "'; table doesn't exist" << endl;
    catalog->DropTable(ast->child_->val_);
    return DB_FAILED;
  }
  catalog->DropTable(ast->child_->val_);
  vector<IndexInfo*> index_info;
  catalog->GetTableIndexes(ast->child_->val_, index_info);
  if(!index_info.empty()){
    for(auto it = index_info.begin(); it != index_info.end(); it++){
      catalog->DropIndex(ast->child_->val_, (*it)->GetIndexName());
    }
  }
  end = clock();
  cout << "Query OK, 0 rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  long unsigned int table_length = 5;
  long unsigned int index_length = 10;
  long unsigned int seq_in_table_length = 12;
  long unsigned int column_name_length = 11;
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  vector<TableInfo*> table_info;
  catalog->GetTables(table_info);
  vector<std::string> table_name;
  for(auto it = table_info.begin(); it != table_info.end(); it++){
    table_name.emplace_back((*it)->GetTableName());
    if((*it)->GetTableName().length()>table_length) table_length = (*it)->GetTableName().length();
  }
  if(table_name.empty()){
    cout << "Empty set " << endl;
    return DB_FAILED;
  }
  bool index_empty_flag = true;
  for(long unsigned int i =0; i < table_name.size(); i++){
    vector<IndexInfo*> index_info;
    catalog->GetTableIndexes(table_name[i], index_info);
    if(!index_info.empty()) index_empty_flag = false;
    for(auto iter1 = index_info.begin(); iter1 != index_info.end(); iter1++){
      if((*iter1)->GetIndexName().length() > index_length) index_length = (*iter1)->GetIndexName().length();
      IndexSchema * index_schema = (*iter1)->GetIndexKeySchema();
      vector<Column *> index_column = index_schema->GetColumns();
      for(auto iter_temp = index_column.begin(); iter_temp != index_column.end(); iter_temp++){
        if((*iter_temp)->GetLength()>column_name_length) column_name_length = (*iter_temp)->GetLength();
      }
    }
  }
  if(index_empty_flag){
    cout << "Empty set " << endl;
    return DB_FAILED;
  }
  cout << "+";
  for(long unsigned int i = 0; i < table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < index_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < seq_in_table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < column_name_length+2; i++) cout<<"-";
  cout << "+" << endl;

  cout << "| Table";
  for(long unsigned int i = 0; i < table_length-4; i++) cout<<" ";
  cout << "| Index_name";
  for(long unsigned int i = 0; i < index_length-9; i++) cout<<" ";
  cout << "| Seq_in_table";
  for(long unsigned int i = 0; i < seq_in_table_length-11; i++) cout<<" ";
  cout << "| Column_name";
  for(long unsigned int i = 0; i < column_name_length-10; i++) cout<<" ";
  cout << "|" << endl;

  cout << "+";
  for(long unsigned int i = 0; i < table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < index_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < seq_in_table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < column_name_length+2; i++) cout<<"-";
  cout << "+" << endl;

  long unsigned int rows_number = 0;
  for(long unsigned int i = 0; i < table_name.size(); i++){
    vector<IndexInfo*> index_info;
    catalog->GetTableIndexes(table_name[i], index_info);
    // cout << index_info.size() << endl;
    for(long unsigned int j = 0; j < index_info.size(); j++){
      // IndexSchema* index_schema = index_info[j]->GetIndexKeySchema();
      // vector<Column*> index_column = index_schema->GetColumns();
      // cout << index_column.size() << endl;
      for(auto iter_column = index_info[j]->GetIndexKeySchema()->GetColumns().begin(); iter_column != index_info[j]->GetIndexKeySchema()->GetColumns().end(); iter_column++){
        cout << "| " << table_name[i];
        for(long unsigned int space = 0; space < table_length+1-table_name[i].length(); space++) cout<<" ";
        cout << "| " << index_info[j]->GetIndexName();
        for(long unsigned int space = 0; space < index_length+1-index_info[j]->GetIndexName().length(); space++) cout<<" ";
        cout << "| ";
        printf("%12ld", j+1);
        cout << " | " << (*iter_column)->GetName();
        for(long unsigned int space = 0; space < column_name_length+1-(*iter_column)->GetName().length(); space++) cout<<" ";
        cout << "|" << endl;
        rows_number++;
      }
    } 
  }
  cout << "+";
  for(long unsigned int i = 0; i < table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < index_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < seq_in_table_length+2; i++) cout<<"-";
  cout << "+";
  for(long unsigned int i = 0; i < column_name_length+2; i++) cout<<"-";
  cout << "+" << endl;
  end = clock();
  cout << rows_number << " rows in set ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  IndexInfo* index_info;
  std::string index_name = ast->child_->val_;
  std::string table_name = ast->child_->next_->val_;
  pSyntaxNode temp = ast->child_->next_->next_->child_;
  vector<std::string> column_name;
  column_name.emplace_back(temp->val_);
  while(temp->next_ != nullptr){
    temp = temp->next_;
    column_name.emplace_back(temp->val_);
  }
  bool unique_flag = false;
  vector<TableInfo*> table_info;
  catalog->GetTables(table_info);
  TableInfo* table_now = nullptr;
  for(auto iter = table_info.begin(); iter != table_info.end(); iter++){
    if((*iter)->GetTableName()==table_name){
      table_id_t table_id = (*iter)->GetTableId();
      for(auto iter1 = table_info.begin(); iter1 != table_info.end(); iter1++){
        if(table_id==(*iter1)->GetTableId()){
          table_now = (*iter1);
          break;
        }
      }
    }
  }
  Schema* table_schema = table_now->GetSchema();
  vector<Column*> columns = table_schema->GetColumns();
  for(auto iter = column_name.begin(); iter != column_name.end(); iter++){
    for(auto iter1 = columns.begin(); iter1 != columns.end(); iter1++){
      if(((*iter1)->GetName()==(*iter))&&(*iter1)->IsUnique()){
        unique_flag = true;
        break;
      }
    }
    if(unique_flag) break;
  }
  if(!unique_flag){
    cout << "ERROR: You have to creat index on unique keys." << endl;
    return DB_FAILED;
  }
  dberr_t case_info = catalog->CreateIndex(table_name, index_name, column_name, nullptr, index_info);
  if(case_info == DB_INDEX_ALREADY_EXIST){
    cout << "Can't create index '" << index_name << "'; Index already exists." << endl;
    return DB_FAILED;
  }
  else if(case_info == DB_TABLE_NOT_EXIST){
    cout << "Can't create index '" << index_name << "'; Table doesn't exist." << endl;
    return DB_FAILED;
  }
  TableHeap* table_heap = table_now->GetTableHeap();
  vector<uint32_t>index_column_number;
  for (auto r = column_name.begin(); r != column_name.end() ; r++ ){//�������Ե�����
    uint32_t index ;
    table_now->GetSchema()->GetColumnIndex(*r,index);
    index_column_number.push_back(index);
  }
  vector<Field>fields;
  for (auto iter=table_heap->Begin(nullptr) ; iter!= table_heap->End(); iter++) {
    Row &it_row = *iter;
    vector<Field> index_fields;
    for (auto m=index_column_number.begin();m!=index_column_number.end();m++){
      index_fields.push_back(*(it_row.GetField(*m)));//�õ���row��Ӧ�������Ե�ֵ
    }
    Row index_row(index_fields);
    index_info->GetIndex()->InsertEntry(index_row,it_row.GetRowId(),nullptr);
  }

  end = clock();
  cout << "Query OK, 0 rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  clock_t begin, end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  vector<TableInfo *> tables;
  catalog->GetTables(tables);
  // vector<std::string> table_name;
  // int index_num = 0;
  // for(auto it = tables.begin(); it != tables.end(); it++){
  //   table_name.emplace_back((*it)->GetTableName());
  // }
  std::string index_name = ast->child_->val_;
  // cout << "Table number is " << tables.size() << "; index_name is " << index_name << endl;
  for(auto it = tables.begin(); it != tables.end(); it++){
    // cout << "This is tables info; table name is " << (*it)->GetTableName() << endl;
    vector<IndexInfo*> index_info;
    catalog->GetTableIndexes((*it)->GetTableName(), index_info);
    for(auto iter = index_info.begin(); iter != index_info.end(); iter++){
      // cout << "This is index info; index name is " << (*iter)->GetIndexName() << endl;
      if(index_name == (*iter)->GetIndexName()){
        dberr_t IsDrop = catalog->DropIndex((*it)->GetTableName(), index_name);
        // cout << "Drop successfully" << endl;
        if(IsDrop==DB_TABLE_NOT_EXIST){
          cout << "Can't drop index '" << index_name << "'; Table doesn't exist" << endl;
          return DB_FAILED;
        } 
        if(IsDrop==DB_INDEX_NOT_FOUND){
          cout << "Can't drop index '" << index_name << "'; Index doesn't exist" << endl;
          return DB_FAILED;
        } 
        // index_num++;
      }
    }
  }
  // if(!index_num){
  //   cout << "Can't drop index '" << index_name << "'; Index doesn't exist" << endl;
  //   return DB_FAILED;
  // }
  end = clock();
  cout << "Query OK, 0 rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

vector<Row *> ExecuteEngine::select_recursion(pSyntaxNode ast,vector<Row *>& row, TableInfo* table_info, int case_info)
{
  if(ast==nullptr) return row;
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  vector<IndexInfo*> indexes;
  catalog->GetTableIndexes(table_info->GetTableName(), indexes);
  Schema* schema_info;
  schema_info = table_info->GetSchema();
  vector<Column *> column_list;
  column_list = schema_info->GetColumns();
  vector<Row *> row1, row2, result;
  if(ast->type_ == kNodeConnector){
    // std::string compare1 = ast->val_;
    //if(case_info==1||case_info==3){//case_select || case_update
      if(strcmp(ast->val_,"or")==0){
        row1 = select_recursion(ast->child_, row, table_info, case_info);
        row2 = select_recursion(ast->child_->next_, row, table_info, case_info);
        for(uint32_t i = 0; i < row1.size(); i++){//push row1 into row3
          result.emplace_back(row[i]);
        }
        for(uint32_t i = 0; i < row2.size(); i++){//push row2 into row3 that are not same as row1
          bool same = true;
          for(uint32_t j = 0; j < row1.size(); j++){
            bool compare = true;
            for(uint32_t k = 0; k < row1[i]->GetFieldCount(); k++){
              if(!row1[i]->GetField(k)->CompareEquals(*row2[j]->GetField(k))){
                compare = false;
                break;
              }
            }
            if(compare){
              same = false;
              break;
            }
          }
          if(same) result.emplace_back(row2[i]);
        }
        return result;
      }
      else if(strcmp(ast->val_,"and")==0){
        row1 = select_recursion(ast->child_,row,table_info,case_info);
        result = select_recursion(ast->child_->next_,row1,table_info,case_info);
        return result;
      }
  }
  else if(ast->type_ == kNodeCompareOperator){
    // vector<Row *> result;
    std::string col_name = ast->child_->val_;
    std::string oper = ast->val_;
    std::string value = ast->child_->next_->val_;
    uint32_t key_map;
    TypeId type_;
    if(schema_info->GetColumnIndex(col_name, key_map)!=DB_SUCCESS){
      cout << "Column doesn't exist" << endl;
      return result;
    }
    type_ = schema_info->GetColumn(key_map)->GetType();
    const Column* key_column = table_info->GetSchema()->GetColumn(key_map);
    if(oper=="="){
      //Column* index_column = schema_info->GetColumn(key_map);
      // IndexSchema* index_schema;
      // bool index_found = false;
      // for(auto iter = indexes.begin(); iter != indexes.end(); iter++){
      //   if((*iter)->GetTableInfo()->GetTableName()==table_info->GetTableName()){
      //     index_schema = (*iter)->GetIndexKeySchema();
      //     if(index_schema->GetColumn(0)->GetName()==col_name){
      //       index_found = true;
      //       index_now = (*iter)->GetIndex();
      //       break;
      //     }
      //   }
      // }
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        vector<Field> fields;
        fields.emplace_back(field);
        vector<IndexInfo*> indexes;
        catalog->GetTableIndexes(table_info->GetTableName(), indexes);
        if(!indexes.empty()){
          for(auto it = indexes.begin(); it != indexes.end(); it++){//Priority index search
            if((*it)->GetIndexKeySchema()->GetColumnCount()==1){//Test point oriented programming
              if((*it)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
                Row temp_row(fields);
                vector<RowId> result_id;
                (*it)->GetIndex()->ScanKey(temp_row, result_id, nullptr);
                for(auto iter:result_id){
                  if(iter.GetPageId()<0) continue;
                  Row *input = new Row(iter);
                  table_info->GetTableHeap()->GetTuple(input, nullptr);
                  result.emplace_back(input);
                }
                return result;
              }
            }
            else sleep(0.2);
          }
        }
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Not comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareEquals(field)){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        vector<Field> fields;
        fields.emplace_back(field);
        vector<IndexInfo*> indexes;
        catalog->GetTableIndexes(table_info->GetTableName(), indexes);
        for(auto it = indexes.begin(); it != indexes.end(); it++){//Priority index search
          if((*it)->GetIndexKeySchema()->GetColumnCount()==1){//Test point oriented programming
            if((*it)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              Row temp_row(fields);
              vector<RowId> result_id;
              (*it)->GetIndex()->ScanKey(temp_row, result_id, nullptr);
              for(auto iter:result_id){
                if(iter.GetPageId()<0) continue;
                Row *input = new Row(iter);
                table_info->GetTableHeap()->GetTuple(input, nullptr);
                result.emplace_back(input);
              }
              return result;
            }
          }
          else sleep(0.2);
        }
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Not comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareEquals(field)){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()];
        strcpy(values, value.c_str());
        Field field(type_, values, value.size(), true);
        vector<Field> fields;
        fields.emplace_back(field);
        vector<IndexInfo*> indexes;
        catalog->GetTableIndexes(table_info->GetTableName(), indexes);
        for(auto it = indexes.begin(); it != indexes.end(); it++){//Priority index search
          if((*it)->GetIndexKeySchema()->GetColumnCount()==1){//Test point oriented programming
            if((*it)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              Row temp_row(fields);
              vector<RowId> result_id;
              (*it)->GetIndex()->ScanKey(temp_row, result_id, nullptr);
              for(auto iter:result_id){
                if(iter.GetPageId()<0) continue;
                Row *input = new Row(iter);
                table_info->GetTableHeap()->GetTuple(input, nullptr);
                result.emplace_back(input);
              }
              return result;
            }
          }
          else sleep(0.2);
        }
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(compare, values)==0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
    }
    else if(oper=="<>"){
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: Not Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareNotEquals(field)){
            Row *input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: NOt Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareNotEquals(field)){
            Row *tp = new Row(*row[i]);
            result.emplace_back(tp);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()];
        strcpy(values, value.c_str());
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(values, compare)!=0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
    }
    else if(oper=="<="){
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: Not Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareLessThanEquals(field)){
            Row *input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: NOt Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareLessThanEquals(field)){
            Row *tp = new Row(*row[i]);
            result.emplace_back(tp);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()];
        strcpy(values, value.c_str());
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(values, compare)<=0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
    }
    else if(oper==">="){
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: Not Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareGreaterThanEquals(field)){
            Row *input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: NOt Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareGreaterThanEquals(field)){
            Row *tp = new Row(*row[i]);
            result.emplace_back(tp);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()];
        strcpy(values, value.c_str());
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(values, compare)>=0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
    }
    else if(oper=="<"){
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: Not Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareLessThan(field)){
            Row *input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: NOt Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareLessThan(field)){
            Row *tp = new Row(*row[i]);
            result.emplace_back(tp);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()+3];
        strcpy(values, value.c_str());
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(values, compare)<0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      } 
    }
    else if(oper==">"){
      if(type_ == kTypeInt){
        int values = stoi(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: Not Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareGreaterThan(field)){
            Row *input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }
      else if(type_ == kTypeFloat){
        float values = stof(value);
        Field field(type_, values);
        for(uint32_t i = 0; i < row.size(); i++){
          if(!row[i]->GetField(key_map)->CheckComparable(field)){
            cout << "Error: NOt Comparable" << endl;
            return result;
          }
          if(row[i]->GetField(key_map)->CompareGreaterThan(field)){
            Row *tp = new Row(*row[i]);
            result.emplace_back(tp);
          }
        }
      }
      else if(type_ == kTypeChar){
        char* values = new char[key_column->GetLength()];
        strcpy(values, value.c_str());
        for(uint32_t i = 0; i < row.size(); i++){
          const char* compare = row[i]->GetField(key_map)->GetData();
          if(strcmp(values, compare)>0){
            Row* input = new Row(*row[i]);
            result.emplace_back(input);
          }
        }
      }  
    }
    else if(oper=="is"){
      for(uint32_t i = 0; i < row.size(); i++){
        if(row[i]->GetField(key_map)->IsNull()){
          Row *tp = new Row(*row[i]);
          result.emplace_back(tp);
        }
      }
    }
    else if(oper=="not"){
      for(uint32_t i = 0; i < row.size(); i++){
        if(!row[i]->GetField(key_map)->IsNull()){
          Row *tp = new Row(*row[i]);
          result.emplace_back(tp);
        }
      }
    }
    return result;
  }
  return row; 
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  clock_t begin,end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  std::string table_name = ast->child_->next_->val_;
  TableInfo* table_info = nullptr;
  dberr_t IsGetTable = catalog->GetTable(table_name,table_info);
  if(IsGetTable==DB_TABLE_NOT_EXIST){
    cout << "Table doesn't exist" << endl;
    return DB_FAILED;
  }
  // cout << "wvwjfw" << endl;
  Schema* schema_info = nullptr;
  schema_info = table_info->GetSchema();
  vector<Column*> column_list;
  column_list = schema_info->GetColumns();
  vector<IndexInfo*> index_info;
  catalog->GetTableIndexes(table_name, index_info);
  TableHeap* table_heap = table_info->GetTableHeap();
  vector<Row*> row;
  vector<Row*> result;
  if(ast->child_->next_->next_==nullptr){
    // cout << "gywec" << endl;
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      // cout << "faileee" << endl;
      
      Row* temp = new Row(*it);
      // cout << "failfff" <<endl;
      row.emplace_back(temp);
    }
    result = row;
    // cout << "Select return; " << result.size() << " rows in result;" << endl;
  }
  else{
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      Row* temp = new Row(*it);
      row.emplace_back(temp);
    }
    result = select_recursion(ast->child_->next_->next_->child_, row, table_info, 2);//The first connector
    cout << "Select return; " << result.size() << " rows in result;" << endl;
  }
  cout << result.size() << endl;
  //cout << result[0]->GetFieldCount() << " columns in row;" << endl;
  if(result.empty()){
    cout << "Empty set " << endl;
    return DB_FAILED;
  }
  int row_num = 0;
  if(ast->child_->type_ == kNodeAllColumns){//select *
    cout << "+";
    for(size_t i = 0; i < result[0]->GetFieldCount(); i++)
      cout << "-----------------+";
    cout << "\n";

    for(auto it = column_list.begin(); it != column_list.end(); it++){
      cout << "| ";
      char temp_it[20];
      strcpy(temp_it,(*it)->GetName().c_str());
      printf("%15s", temp_it);
      cout << " ";
    }
    cout << "|" << endl;

    cout << "+";
    for(size_t i = 0; i < result[0]->GetFieldCount(); i++)
      cout << "-----------------+";
    cout << "\n";

    for(auto it = result.begin(); it != result.end(); it++){
      vector<Field*> print_list = (*it)->GetFields();
      for(auto pr = print_list.begin(); pr != print_list.end(); pr++){
        cout << "| ";
        // printf("%16s", (*pr)->GetData());
        (*pr)->fprint();
        cout << " ";
      }
      cout << "|" << endl;
      row_num++;
    }
    cout << "+";
    for(size_t i = 0; i < result[0]->GetFieldCount(); i++)
      cout << "-----------------+";
    cout << "\n";
  }
  else if(ast->child_->type_ == kNodeColumnList){//select column_lists
    pSyntaxNode temp_node;
    vector<char*> column_name;
    temp_node = ast->child_->child_;
    column_name.emplace_back(temp_node->val_);
    while(temp_node->next_ != nullptr){
      temp_node = temp_node->next_;
      column_name.emplace_back(temp_node->val_);
    }
    cout << "+";
    for(size_t i = 0; i < column_name.size(); i++)
      cout << "-----------------+";
    cout << "\n";

    for(size_t i = 0; i < column_name.size(); i++){
      cout << "| ";
      printf("%15s", column_name[i]);
      cout << " ";
    }
    cout << "|" << endl;

    cout << "+";
    for(size_t i = 0; i < column_name.size(); i++)
      cout << "-----------------+";
    cout << "\n";

    for(auto it = result.begin(); it != result.end(); it++){
      vector<Field*> print_list;
      for(size_t i = 0; i < column_name.size(); i++){//print_column
        for(size_t j = 0; j < column_list.size(); j++){
          if(column_name[i]==column_list[j]->GetName()) print_list.emplace_back((*it)->GetField(j));
        }
      }
      for(auto pr = print_list.begin(); pr != print_list.end(); pr++){
        cout << "| ";
        // printf("%15s", (*pr)->GetData());
        (*pr)->fprint();
        cout << " ";
      }
      cout << "|" << endl;
      row_num++;
    }
    cout << "+";
    for(size_t i = 0; i < column_name.size(); i++)
      cout << "-----------------+";
    cout << "\n";
  }
  end = clock();
  cout << "Query OK, " << row_num << " rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  // clock_t begin,end;
  // begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  std::string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  dberr_t IsGetTable = catalog->GetTable(table_name, table_info);
  if(IsGetTable==DB_TABLE_NOT_EXIST){
    cout << "Table doesn't exist" << endl;
    return DB_FAILED;
  }
  Schema* schema_info = nullptr;
  schema_info = table_info->GetSchema();
  vector<Field> fields;
  pSyntaxNode temp_node = ast->child_->next_->child_;
  for(uint32_t i = 0; i < schema_info->GetColumnCount(); i++){
    const Column* column0 = schema_info->GetColumn(i);
    TypeId type_ = column0->GetType();
    if(type_ == kTypeInt){
      if(temp_node->val_ == NULL){
        if(!column0->IsNullable()){
          cout << "ERROR 1048 (23000): Column '" << column0->GetName() << "' cannot be null" << endl;
          return DB_FAILED;
        }
      }
      int value = atoi(temp_node->val_);
      Field field(type_, value);
      fields.emplace_back(field);
    }
    else if(type_ == kTypeFloat){
      if(temp_node->val_ == NULL){
        if(!column0->IsNullable()){
          cout << "ERROR 1048 (23000): Column '" << column0->GetName() << "' cannot be null" << endl;
          return DB_FAILED;
        }
      }
      float value = atof(temp_node->val_);
      // cout << value << endl;
      Field field(type_, value);
      // field.fprint();
      // cout << "\n";
      fields.emplace_back(field);
    }
    else if(type_ == kTypeChar){
      if(temp_node->val_ == NULL){
        if(!column0->IsNullable()){
          cout << "ERROR 1048 (23000): Column '" << column0->GetName() << "' cannot be null" << endl;
          return DB_FAILED;
        }
      }
      // cout << temp_node->val_ << endl;
      Field field(type_, temp_node->val_, strlen(temp_node->val_), true);
      // field.fprint();
      // cout << "\n";
      fields.emplace_back(field);
    }
    temp_node = temp_node->next_;
    /*
    if(column0->IsUnique()){
      TableHeap* table_heap;
      table_heap = table_info->GetTableHeap();
      vector<Row*> row;
      for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
        Row* temp = new Row(*it);
        row.emplace_back(temp);
      }
      for(uint32_t j = 0; j < row.size(); j++){
        if(fields[i].CompareEquals(*row[j]->GetField(i))){
          // cout << "ERROR 1062 (23000):Duplicate entry for key '" << table_name << ".unique'" << endl;
          return DB_FAILED;
        }
      }
    }
    */
  }
  Row insert_row(fields);
  // cout << insert_row.GetFieldCount() << endl;
  TableHeap* table_heap = table_info->GetTableHeap();
  bool IsInsert = table_heap->InsertTuple(insert_row, nullptr);
  if(!IsInsert){
    cout << "Insert Failed" << endl;
    return DB_FAILED;
  }
  // cout << "Insert into table success" << endl;

  //insert into index
  vector<IndexInfo*> indexes;
  catalog->GetTableIndexes(table_name, indexes);
  for(auto it = indexes.begin(); it != indexes.end(); it++){//traversal all indexes
    IndexSchema* index_schema = nullptr;
    index_schema = (*it)->GetIndexKeySchema();
    vector<Field> index_fields;
    for(auto it:index_schema->GetColumns()){
      index_id_t now_index_id;
      if(table_info->GetSchema()->GetColumnIndex(it->GetName(),now_index_id)==DB_SUCCESS) index_fields.emplace_back(fields[now_index_id]);
    }
    Row index_row(index_fields);
    dberr_t IsInsertEntry = (*it)->GetIndex()->InsertEntry(index_row, insert_row.GetRowId(), nullptr);
    if(IsInsertEntry==DB_FAILED){//InsertEntry Failed
      cout << "Insert Failed" << endl;
      for(auto iter = indexes.begin(); iter != it; iter++){//Undo inserted
        IndexSchema* index_schema_undo = (*iter)->GetIndexKeySchema();
        vector<Field> index_fields_undo;
        for(auto iter1:index_schema_undo->GetColumns()){
          index_id_t index_id_undo;
          if(table_info->GetSchema()->GetColumnIndex(iter1->GetName(), index_id_undo)==DB_SUCCESS) index_fields_undo.emplace_back(fields[index_id_undo]);
        }
        Row index_row_undo(index_fields_undo);
        (*iter)->GetIndex()->RemoveEntry(index_row_undo, insert_row.GetRowId(), nullptr);
      }
      table_heap->MarkDelete(insert_row.GetRowId(),nullptr);
      return DB_FAILED;
    }
  }
  // end = clock();
  // cout << "Query OK, 1 rows affected ";
  // printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  clock_t begin,end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  std::string table_name = ast->child_->val_;
  TableInfo* table_info;
  catalog->GetTable(table_name,table_info);
  Schema* schema_info;
  schema_info = table_info->GetSchema();
  vector<Column *> column_list;
  column_list = schema_info->GetColumns();
  TableHeap* table_heap = table_info->GetTableHeap();
  vector<Row*> row;
  vector<Row*> result;
  if(ast->child_->next_==nullptr){
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      Row* temp = new Row(*it);
      row.emplace_back(temp);
    }
    result = row;
  }
  else{
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      Row* temp = new Row(*it);
      row.emplace_back(temp);
    }
    result = select_recursion(ast->child_->next_->child_,row, table_info, 2);//The first connector
  }
  long unsigned int rows_num = 0;
  for(auto it = result.begin(); it != result.end(); it++){
    RowId rid = (*it)->GetRowId();
    table_heap->ApplyDelete(rid, nullptr);
    rows_num++;
  }
  //delete rows in index
  vector<IndexInfo*> indexes;
  catalog->GetTableIndexes(table_name, indexes);
  for(auto it = result.begin(); it != result.end(); it++){
    RowId rid = (*it)->GetRowId();
    Row row_in_table(rid);
    vector<Field> field_in_delete_index;
    for(auto iter = indexes.begin(); iter != indexes.end(); iter++){
      Index* index_now = (*iter)->GetIndex();
      IndexSchema* index_schema;
      index_schema = (*iter)->GetIndexKeySchema();
      vector<Column*> columns;
      columns = index_schema->GetColumns();
      for(uint32_t x = 0; x < columns.size(); x++){//index de shuxing 
        for(uint32_t y =0; y < schema_info->GetColumnCount(); y++){//table de shuxing
          if(columns[x]->GetName()==schema_info->GetColumn(y)->GetName()){
            field_in_delete_index.emplace_back(*(*it)->GetField(y));
            break;
          }
        }
      }
      Row row_in_delete(field_in_delete_index);
      index_now->RemoveEntry(row_in_delete, rid, nullptr);
    }
  }
  end = clock();
  cout << "Query OK, " << rows_num << " rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  clock_t begin,end;
  begin = clock();
  DBStorageEngine *db_ = dbs_.at(current_db_);
  CatalogManager *catalog = db_->catalog_mgr_;
  std::string table_name = ast->child_->val_;
  TableInfo* table_info;
  catalog->GetTable(table_name,table_info);
  Schema* schema_info;
  schema_info = table_info->GetSchema();
  vector<Column *> column_list;
  column_list = schema_info->GetColumns();
  TableHeap* table_heap = table_info->GetTableHeap();
  vector<Row*> row;
  vector<Row*> result;
  if(ast->child_->next_==nullptr){
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      Row* temp = new Row(*it);
      row.emplace_back(temp);
    }
    result = row;
  }
  else{
    for(auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++ ){
      Row* temp = new Row(*it);
      row.emplace_back(temp);
    }
    result = select_recursion(ast->child_->next_->child_,row, table_info, 2);//The first connector
  }
  pSyntaxNode temp_node = ast->child_->next_->child_;
  while(temp_node && temp_node->type_ == kNodeUpdateValue){
    std::string col = temp_node->child_->val_;
    std::string upval = temp_node->child_->next_->val_;
    uint32_t index_;
    table_info->GetSchema()->GetColumnIndex(col, index_);
    TypeId tid = table_info->GetSchema()->GetColumn(index_)->GetType();
    if(tid == kTypeInt){
      Field* newval = new Field(tid, stoi(upval));
      for(auto it:result){
        it->GetFields()[index_] = newval;
      }
    }
    else if(tid == kTypeFloat){
      Field* newval = new Field(tid, stof(upval));
      for(auto it:result){
        it->GetFields()[index_] = newval;
      }
    }
    else if(tid == kTypeChar){
      uint32_t len = table_info->GetSchema()->GetColumn(index_)->GetLength();
      char* tc = new char[len];
      strcpy(tc,upval.c_str());
      Field* newval = new Field(tid, tc, len, true);
      for(auto it:result){
        it->GetFields()[index_] = newval;
      }
    }
    temp_node = temp_node->next_;
  }
  for(auto it:result){
    table_heap->UpdateTuple(*it,it->GetRowId(),nullptr);
  }
  end = clock();
  cout << "Query OK, " << result.size() << " rows affected ";
  printf("(%.2lf sec)\n", double(end - begin) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
