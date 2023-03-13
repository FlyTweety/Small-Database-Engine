#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  // index_id_t index_id_;
  // std::string index_name_;
  // table_id_t table_id_;
  // std::vector<uint32_t> key_map_;
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf+offset, INDEX_METADATA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(index_id_t, buf+offset, index_id_);
  offset += sizeof(uint32_t);
  uint32_t len = index_name_.size(); 
  memcpy(buf+offset, &len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf+offset, index_name_.c_str(), len);
  offset += len;
  MACH_WRITE_TO(table_id_t, buf+offset, table_id_);
  offset += sizeof(uint32_t);
  len = key_map_.size();
  MACH_WRITE_TO(uint32_t, buf+offset, len);
  offset += sizeof(uint32_t);
  for (uint32_t i=0; i<len; i++) {
    MACH_WRITE_TO(uint32_t, buf+offset, key_map_[i]);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return  index_name_.size() + sizeof(uint32_t) * key_map_.size() + 5*sizeof(uint32_t);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  // index_id_t index_id_;
  // std::string index_name_;
  // table_id_t table_id_;
  // std::vector<uint32_t> key_map_;
  index_id_t index_id;
  string index_name;
  table_id_t table_id;
  vector<uint32_t> key_map;
  uint32_t offset = 0;
  uint32_t magic_unm = MACH_READ_FROM(uint32_t, buf+offset);
  if (magic_unm != INDEX_METADATA_MAGIC_NUM) ASSERT(false, "error");
  offset += sizeof(uint32_t);
  index_id = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  uint32_t len = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  index_name.append(buf+offset,len);
  offset += len;
  table_id = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  len = MACH_READ_FROM(uint32_t, buf+offset);
  offset += sizeof(uint32_t);
  for(size_t i=0;i< len ; i++){
    uint32_t tmp = MACH_READ_FROM(uint32_t, buf+offset);
    offset += sizeof(uint32_t);
    key_map.push_back(tmp);
  }
  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
  return offset;
}
