#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  //return sizeof(u_int32_t);
  u_int32_t offset = 0;
  u_int32_t offsetMap = 0;
  u_int32_t count = GetFieldCount();
  bool zero = false;
  bool one = true;
  memcpy(buf+offset, &count, sizeof(uint32_t));
  offset = offset + sizeof(u_int32_t);
  offsetMap = offset;
  //存储nullbitmap的长�?
  offset = offset + count;
  //预留出这么长的位置给nullbitmap
  for(uint32_t i = 0;i<GetFieldCount();i++){
    if(fields_[i]->IsNull()){
      memcpy(buf+offsetMap, &zero, sizeof(bool));
    }
    else{
      memcpy(buf+offsetMap, &one, sizeof(bool));
    }
    offsetMap = offsetMap + sizeof(bool);
    offset = offset + fields_[i]->SerializeTo(buf + offset);
  }
  return offset;
   /*
  for(uint32_t i = 0;i<GetFieldCount();i++){
    if(fields_[i]->IsNull()){
      memcpy(buf+offset, &zero, 1);
      offset = offset + 1;
    }
    else{
      memcpy(buf+offset, &one, 1);
      offset = offset + 1;
    }
  }
  for(uint32_t i = 0;i<GetFieldCount();i++){
    if(fields_[i]->IsNull()){
      continue;
    }
    else{
      offset = offset + fields_[i]->SerializeTo(buf + offset);
    }
  }
  return offset;
  */
  /*
  使用nullbitmap存储
  存储nullbitmap长度
  存储nullbitmap内容
  存储所有非null的field数据
  */
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  //return sizeof(u_int32_t);
  bool Flag;
  TypeId type_id;
  u_int32_t offsetMap = 0, offset = 0;
  u_int32_t count = MACH_READ_UINT32(buf);
  offset = offset + sizeof(u_int32_t);
  offsetMap = offset;
  offset = offset + count;
  //首先读取nullbitmap的长度count
  //offsetMap专门用来存储nullbitmap的位�?
  for(u_int32_t i = 0;i<count;i++){
    Field **p;
    p = (Field **)malloc(sizeof(Field));
    //类似schema中的步骤
    Flag = (*reinterpret_cast<const bool *>(buf + offsetMap));
    offsetMap = offsetMap + sizeof(bool);
    //用来找nullbitmap的值，判断is_null
    type_id = schema->GetColumn(i)->GetType();
    //调用fieldDeserialized
    offset = offset + (*p)->DeserializeFrom(buf + offset,type_id, p, !Flag, heap_);
    fields_.push_back(*p);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  //return sizeof(u_int32_t);
  u_int32_t offset = 0;
  offset = offset + sizeof(u_int32_t) + GetFieldCount()*sizeof(bool);
  for(uint32_t i = 0;i<GetFieldCount();i++){
    offset = offset + fields_[i]->GetSerializedSize();
  }
  return offset;
}

