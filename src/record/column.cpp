#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  //ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {

  uint32_t len = name_.size();
  memcpy(buf, &len, sizeof(uint32_t));
  name_.copy(buf + sizeof(u_int32_t), len, 0);

  u_int8_t type;
  switch (type_)
  {
    case kTypeInvalid: {type = 0;break;}
    case kTypeInt:     {type = 1;break;}
    case kTypeFloat:   {type = 2;break;}
    case kTypeChar:    {type = 3;break;}
    default:           {type = 0;break;}
  }
  memcpy(buf + sizeof(uint32_t) + len, &type, sizeof(u_int32_t));
  
  memcpy(buf + sizeof(uint32_t) + len + sizeof(u_int8_t), &len_, sizeof(u_int32_t));
  memcpy(buf + 2 * sizeof(u_int32_t) + len + sizeof(u_int8_t),&table_ind_,sizeof(u_int32_t));
  memcpy(buf + 3 * sizeof(u_int32_t) + len + sizeof(u_int8_t),&nullable_,sizeof(bool));
  memcpy(buf + 3 * sizeof(u_int32_t) + len + sizeof(u_int8_t) + sizeof(bool),&unique_,sizeof(bool));
  return len + 3 * sizeof(u_int32_t) + 2 * sizeof(bool) + sizeof(u_int8_t);
  //serializeTo
  //gettypesize
}

uint32_t Column::GetSerializedSize() const {
  uint32_t len = name_.size();
  return len + 3 * sizeof(u_int32_t) + 2 * sizeof(bool) + sizeof(u_int8_t);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  /*读取
  len
  name_
  type
  table_ind
  nullable
  unique
  */
  u_int32_t offset = 0;
  u_int32_t name_len = MACH_READ_UINT32(buf);
  offset = offset + sizeof(u_int32_t);

  std::string column_name;
  column_name.append(buf + offset, name_len);
  offset = offset + name_len;

  u_int8_t type = MACH_READ_FROM(u_int8_t, buf + offset);
  offset = offset + sizeof(u_int8_t);
  TypeId stype;
  switch (type)
  {
    case 0:  {stype = kTypeInvalid;break;}
    case 1:  {stype = kTypeInt;break;}
    case 2:  {stype = kTypeFloat;break;}
    case 3:  {stype = kTypeChar;break;}
    default: {stype = kTypeInvalid;break;}
  }

  u_int32_t len = MACH_READ_UINT32(buf + offset);
  offset = offset + sizeof(u_int32_t);
  u_int32_t table_ind = MACH_READ_UINT32(buf + offset);
  offset = offset + sizeof(u_int32_t);
  bool nullable = MACH_READ_FROM(bool, buf + offset);
  offset = offset + sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf + offset);
  offset = offset + sizeof(bool);
  
  void *mem = heap->Allocate(sizeof(Column));
  if(type==kTypeChar)
  column = new(mem)Column(column_name, stype, len, table_ind, nullable, unique);
  else
  column = new(mem)Column(column_name, stype, table_ind, nullable, unique);
  return offset;
}
