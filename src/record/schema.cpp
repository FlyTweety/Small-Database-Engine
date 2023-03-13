#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  u_int32_t offset = 0;
  u_int32_t count = GetColumnCount();
  //store the column count
  memcpy(buf, &count, sizeof(uint32_t));
  offset = offset + sizeof(u_int32_t);
  //uint32_t len;
  for(u_int32_t i = 0;i<GetColumnCount();i++){
    /*
    memcpy(buf + offset, &((*columns_[i]).type_), sizeof(u_int32_t));
    offset = offset + sizeof(u_int32_t);
    */
    offset = offset + columns_[i]->SerializeTo(buf + offset);
  }
  /*
  for(u_int32_t i = 0;i<GetColumnCount();i++){
    len = columns_[i]->GetLength();
    memcpy(buf + offset, &len, sizeof(uint32_t));
    memcpy(buf + offset + sizeof(uint32_t),&((*columns_[i]).name_),len);
    offset = offset + len + sizeof(uint32_t);
  }
  */
  return offset;
}
uint32_t Schema::GetSerializedSize() const {
  uint32_t offset = 0;
  offset = offset + sizeof(u_int32_t);
  for(uint32_t i = 0;i<GetColumnCount();i++){
    offset = offset + columns_[i]->GetSerializedSize();
  }
  // replace with your code here
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  u_int32_t offset = 0;
  u_int32_t count = MACH_READ_UINT32(buf);
  offset = offset + sizeof(u_int32_t);
  std::vector<Column *> cols;
  for(u_int32_t i = 0;i<count;i++){
    Column *p;
    p = (Column *)malloc(sizeof(Column));
    offset = offset + p->DeserializeFrom(buf+offset, p, heap);
    cols.push_back(p);
    /*新建一个column变量，作为变量传入column::DeserializeFrom,
    offset = offset + column::DeserializeFrom(buf, column, heap);
    buf每次移动offset
    */
  }
  schema = new(buf) Schema(cols);
  return offset;
}
