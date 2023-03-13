#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  // table_id_t table_id_;
  // std::string table_name_;
  // page_id_t root_page_id_;
  // Schema *schema_;
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf+offset, TABLE_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(table_id_t, buf+offset,table_id_);
  offset += sizeof(uint32_t);
  uint32_t len = table_name_.size(); 
  memcpy(buf+offset, &len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf+offset, table_name_.c_str(), len);
  offset += len;
  MACH_WRITE_TO(int32_t, buf+offset, root_page_id_);
  offset += sizeof(int32_t);
  offset += schema_->SerializeTo(buf+offset);

  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return 4*sizeof(uint32_t) + table_name_.size() + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  // table_id_t table_id_;
  // std::string table_name_;
  // page_id_t root_page_id_;
  // Schema *schema_;
  table_id_t table_id;
  std::string table_name;
  page_id_t root_page_id;
  TableSchema *schema;
  uint32_t offset = 0;
  uint32_t magic_unm = MACH_READ_FROM(uint32_t, buf+offset);
  if (magic_unm != TABLE_METADATA_MAGIC_NUM) ASSERT(false, "error");
  offset += sizeof(uint32_t);
  table_id = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  uint32_t len = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  table_name.append(buf+offset, len);
  offset += len;
  root_page_id = MACH_READ_FROM(int32_t, buf+offset);
  offset += sizeof(int32_t);
  offset += schema->DeserializeFrom(buf+offset,schema,heap);

  void *mem = heap->Allocate(sizeof(TableMetadata));
  table_meta = new(mem)TableMetadata(table_id, table_name, root_page_id, schema);
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
