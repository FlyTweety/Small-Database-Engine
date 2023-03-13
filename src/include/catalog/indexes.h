#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
                           index_id_ = index_id;
                           index_name_ = index_name;
                           table_id_ = table_id;
                           key_map_ = key_map;
                         }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    meta_data_ = meta_data;
    table_info_ = table_info;
    // Step2: mapping index key to key schema
    key_schema_ = Schema::ShallowCopySchema(table_info->GetSchema(), meta_data_->GetKeyMapping(), heap_);
    // Step3: call CreateIndex to create the index
    vector<Column*> cols = key_schema_->GetColumns();
    size_t maxlen = 0;
    //cout <<  "cols="<<key_schema_->GetColumnCount() <<endl;
    for (uint32_t i = 0; i < cols.size(); i++) {
      if (maxlen < cols[i]->GetLength())
        maxlen = cols[i]->GetLength();
    }
    //cout << "maxlen="<< maxlen << endl;
    //size_t len;
    // if (maxlen <= 4) {
    //   using INDEX_KEY_TYPE = GenericKey<4>;
    //   using INDEX_COMPARATOR_TYPE = GenericComparator<4>;
    //   void* mem = heap_->Allocate(sizeof(BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>));
    //   Index *index = new(mem)BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>(meta_data_->GetIndexId(), key_schema_,buffer_pool_manager);
    //   index_ = index;
    // }
    // else if (maxlen <= 8) {
    //   using INDEX_KEY_TYPE = GenericKey<8>;
    //   using INDEX_COMPARATOR_TYPE = GenericComparator<8>;
    //   void* mem = heap_->Allocate(sizeof(BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>));
    //   Index *index = new(mem)BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>(meta_data_->GetIndexId(), key_schema_,buffer_pool_manager);
    //   index_ = index;
    // }
    // else if (maxlen <= 16) {
    //   using INDEX_KEY_TYPE = GenericKey<16>;
    //   using INDEX_COMPARATOR_TYPE = GenericComparator<16>;
    //   void* mem = heap_->Allocate(sizeof(BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>));
    //   Index *index = new(mem)BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>(meta_data_->GetIndexId(), key_schema_,buffer_pool_manager);
    //   index_ = index;
    // }
    if (maxlen <= 32) {
      using INDEX_KEY_TYPE = GenericKey<32>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<32>;
      void* mem = heap_->Allocate(sizeof(BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>));
      Index *index = new(mem)BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>(meta_data_->GetIndexId(), key_schema_,buffer_pool_manager);
      index_ = index;
    }
    else {
      using INDEX_KEY_TYPE = GenericKey<64>;
      using INDEX_COMPARATOR_TYPE = GenericComparator<64>;
      void* mem = heap_->Allocate(sizeof(BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>));
      Index *index = new(mem)BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>(meta_data_->GetIndexId(), key_schema_,buffer_pool_manager);
      index_ = index;
    }
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  
private:
  IndexMetadata *meta_data_;//索引定义时的元信息，除了这个，都是通过反序列化生成的
  Index *index_;//索引操作对象
  TableInfo *table_info_;//表信息
  IndexSchema *key_schema_;//索引的模式信息
  MemHeap *heap_;//维护和管理与自身相关的内存分配和回收
};

#endif //MINISQL_INDEXES_H
