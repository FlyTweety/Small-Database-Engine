#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "page/b_plus_tree_leaf_page.h"

//Unpin娴嬭瘯鏈夐棶棰樸€傚彾瀛愯妭鐐筸ax_size_ = max_size鏃跺氨鍑洪棶棰橈紝max_size-1灏眔k

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {

  root_page_id_ = INVALID_PAGE_ID;   
  IndexRootsPage *rootrootpage=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  bool flag = rootrootpage->GetRootId(index_id,&root_page_id);
  if(flag==true) root_page_id_ = root_page_id;
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);

}

//杩欎釜涓嶇煡閬撴槸锟??
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {

}

/*
 * Helper function to decide whether current b+tree is empty
 */
//鍒ゆ柇鏄惁涓虹┖
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  else return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
//缁欏畾key锛岃繑鍥瀡alue
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  if(IsEmpty() == true) return false; //鍏堟鏌ユ槸鍚︿负锟??
  ValueType value; //璁板綍value
  auto* leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, false));//杩欎釜鍚庨潰leftmost鑷繁鎯崇殑
  bool is_found = leaf_page->Lookup(key, value, comparator_);
  result.push_back(value); //瀛樺叆vector
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return is_found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//鏈€澶栧眰鐨勬彃锟??
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //std::cout<<"锟斤拷锟斤拷insert"<<std::endl;
  if(IsEmpty() == true){//濡傛灉绌哄氨鏂板缓瀹屼簨
    StartNewTree(key, value);
    //cout << "jieshu start new tree" << endl;
    return true;
  }
  else{ //鍒板彾瀛愰噷鍘诲皾璇曟彃锟??
    bool is_success_insert = InsertIntoLeaf(key, value, transaction);
    return is_success_insert;
  }
//鎻掑叆DEBUG杈撳嚭鈥斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€旓拷?
 /*
  ofstream outfile;
  outfile.open("bplustreegarph_insert.txt");
  auto *new_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  ToGraph(root_page, buffer_pool_manager_, outfile);
  ToString(root_page, buffer_pool_manager_);
*/
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
//鏂板缓涓€妫垫爲锛岃繕瑕佹洿鏂皉oot
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  //鍚慴uffer_pool_manager_瑕佷竴涓〉锟??
  //std::cout<<"锟斤拷锟斤拷startnewtree,锟斤拷前root_page_id_锟斤拷"<<root_page_id_<<std::endl;
  Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
  //std::cout<<"NewPage锟斤拷root_page_id_锟斤拷"<<root_page_id_<<std::endl;
  if(new_page == nullptr){
    //std::cout<<"锟斤拷锟截匡拷指锟斤拷"<<std::endl;
    throw std::exception();//棰濓紝鎴戜篃涓嶇煡閬撴姏鍑轰簡浠€涔堬紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锛燂紵锟??
  }
  //鍒濆鍖栨柊鑾峰彇鐨勯〉锟??
  auto* root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID, LEAF_PAGE_SIZE); //鐓х潃鍙跺瓙鑺傜偣鐨処nit
  //鐒跺悗姝ｅ父鎻掑叆
  root_page->Insert(key, value, comparator_);
  //鍦↖ndexRootsPage閲屾洿锟??
  UpdateRootPageId(1); //1灏辨槸鏂板缓鐨勬剰锟??
  buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//鍏堟壘鍒板搴旂殑鍙跺瓙鑺傜偣锛岀劧鍚庡垽鏂槸鍚﹀凡缁忓瓨鍦紝鐒跺悗鎻掑叆锛屽寘鍚垎瑁傘€傚鏋滄槸閲嶅鐨勮瘽灏辫繑鍥瀎alse
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //鎵惧埌瀵瑰簲鐨勫彾瀛愯妭锟??
  auto* leaf_node = FindLeafPage(key, false);
  LeafPage* leaf_page = reinterpret_cast<LeafPage *>(leaf_node->GetData());
  //灏濊瘯鎻掑叆銆傝繖涓狟_PLUS_TREE_LEAF_PAGE_TYPE::Insert鐨勮繑鍥炴槸鎻掑叆鍚庣殑size锛岃繖涓€姝ヨ鍒ゆ柇鏄惁鎻掑叆鎴愬姛
  if(leaf_page->GetSize() == leaf_page->Insert(key, value, comparator_)) {//鎻掑叆澶辫触
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false; 
  }
  //鐜板湪宸茬粡鎻掑叆浜嗭紝鑰冭檻瑕佷笉瑕佸垎锟??
  if(leaf_page->GetSize() > leaf_page->GetMaxSize()){
    auto* new_page = Split(leaf_page);
    //鍒嗚涔嬪悗闇€瑕佸湪鐖舵瘝閲屾彃锟??
    InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  }
  //unpin
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
//鍒嗚鍑轰竴涓柊椤碉紝鎷疯礉涓€鍗婅繃鍘伙紝瑙ｅ喅鐖舵瘝瀛╁瓙鐨勯棶锟??
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  //鑾峰彇涓€涓柊鐨勯〉
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  assert(new_page != nullptr); //杩欓噷娌℃湁鎶涘嚭寮傚父锛岃€屾槸鏀圭敤assert
  N* new_node = reinterpret_cast<N *>(new_page->GetData());
  //鍒濆鍖栨柊锟??
  new_node->Init(new_page_id, node->GetParentPageId());
  //灏嗗師鏈夌殑涓€鍗婄Щ鍒版柊椤点€侻oveHalfTo闈炲父濂界敤锛屾槸鍐呴儴鑺傜偣鐨勮瘽鏄嚜甯﹁皟鏁村瀛愪滑鐨勶紝鍙跺瓙鑺傜偣鐨勮瘽灏辨槸鍗曠函鐨勬尓銆傚悓鏃朵篃鏄嚜甯﹁皟鏁翠袱鑰呭ぇ锟??
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  //杩斿洖鏂扮殑锟??
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//鍥犱负鍙跺瓙鑺傜偣鍒嗚浜嗭紝鎵€浠ヨ鎶婁竴涓柊鐨刱ey鎻掑埌鐖舵瘝鑺傜偣锟?? 杩欏寘鎷簡鐖舵瘝鑺傜偣閫掑綊鐨勬彃锟??
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  //鍏堟壘鍒扮埗姣嶉〉锟??                                      
  page_id_t parent_page_id = old_node->GetParentPageId();
  //鐖舵瘝濡傛灉涓嶅瓨鍦ㄥ氨鏂板缓涓€涓紝鐖舵瘝瀛樺湪灏卞彲鑳介€掑綊
  if(parent_page_id != INVALID_PAGE_ID){ //鏈夌埗姣嶆槸瀛樺湪锟??
    //鍙栧緱鍘熺埗姣嶉〉锟??
    Page *new_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto *parent_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    //insert_after_node鏄笉浼氳€冭檻瓒婄晫鐨勶紝杩欓噷鍏堟彃鍏ワ紝瓒婄晫鐨勮瘽鍐嶈
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    //妫€鏌ヨ秺鐣屻€傚鏋滃師鏉ョ埗姣嶅氨鏄弧鐨勮繖閲岃秺鐣岋紝鍒欓渶瑕侀€掑綊鍦拌皟鐢⊿plit
    if(parent_page->GetSize() > parent_page->GetMaxSize()){ 
      InternalPage* parent_sibling_page = Split(parent_page);
      InsertIntoParent(parent_page, parent_sibling_page->KeyAt(0), parent_sibling_page, transaction);
      buffer_pool_manager_->UnpinPage(parent_sibling_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  else{
    //鑾峰彇鏂扮殑鐖舵瘝椤甸潰
    Page *new_page = buffer_pool_manager_->NewPage(parent_page_id);
    if (new_page == nullptr) {
      throw std::exception();
    }
    auto* new_parent_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    //鍒濆鍖栫埗姣嶉〉锟??
    new_parent_page->Init(parent_page_id, INVALID_PAGE_ID);
    new_parent_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    //鏇存柊鏍硅妭鐐圭殑page_id
    root_page_id_ = parent_page_id; 
    //鏇存柊涓嬮潰涓や釜鑺傜偣鐨勭埗姣嶄负鐜板湪鐨勭埗锟??
    old_node->SetParentPageId(new_parent_page->GetPageId());
    new_node->SetParentPageId(new_parent_page->GetPageId());
    //鍦↖ndexRootsPage閲屾洿锟??
    UpdateRootPageId(0); //1鏄彃锟??0鏄洿锟??
    //unpin new_parent_page锛屾槸鑷繁鍔犵殑
    buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
//杩欎釜濂藉儚灏辨槸鍒犻櫎鍑芥暟鏈€澶栧眰鐨勫叆鍙ｏ紝鍚堝苟鍜宺edisturbe閮藉湪瀛愬嚱鏁拌皟锟??
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  //濡傛灉涓虹┖鍒欑珛鍗宠繑锟??
  if(IsEmpty()) return;
  //鎵惧埌瀵瑰簲鐨勫彾瀛愯妭锟??
  auto* leaf_node = FindLeafPage(key, false); //鏉ヨ嚜浜庡垎涓夐儴鍒嗙殑鍊熼壌
  LeafPage* leaf_page = reinterpret_cast<LeafPage *>(leaf_node->GetData());
  //鍒犻櫎銆俁emoveAndDeleteRecord鍙仛鍒犻櫎鍜岃皟鏁村ぇ灏忥紝涓嶅共鍒殑銆傝繑鍥炲垹闄ゅ悗鐨剆ize
  //濡傛灉鎴愬姛鍒犻櫎浜嗭紝灏卞幓鍒ゆ柇鏄悎骞惰繕鏄噸鍒嗛厤锛屾垨鑰呬篃鍙互鐩存帴杩涘叆CoalesceOrRedistribute锛屽湪閭ｉ噷闈㈠垽鏂涓嶈锟??
  int old_size = leaf_page->GetSize();

  //鍦ㄥ垹闄よ繖閲岃瑙ｅ喅鍒犻櫎鍙跺瓙绗竴涓€屽湪鐖舵瘝閲岃皟鏁寸殑闂锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锛侊紒锟??
  int temp_index = leaf_page->KeyIndex(key, comparator_);
  if( temp_index==0 && leaf_page->GetParentPageId()!=INVALID_PAGE_ID){
      auto *page = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
      auto *parent_page = reinterpret_cast<InternalPage *>(page->GetData()); 
      int change_index = parent_page->ValueIndex(leaf_page->GetPageId());
      KeyType change_key = leaf_page->KeyAt(1);
      InternalPage *ori_page;
      //杩欓噷搴旇鏀规垚閫掑綊鍚戜笂锟??
      while(change_index==0 && parent_page->GetParentPageId()!=INVALID_PAGE_ID){ //璇存槑杩樺湪鏇翠笂锟??
        page = buffer_pool_manager_->FetchPage(parent_page->GetParentPageId());
        ori_page = parent_page;
        parent_page = reinterpret_cast<InternalPage *>(page->GetData()); 
        change_index = parent_page->ValueIndex(ori_page->GetPageId());
        buffer_pool_manager_->UnpinPage(ori_page->GetPageId(), false);
      }
      if(parent_page->GetParentPageId()!=INVALID_PAGE_ID){
        parent_page->SetKeyAt(change_index, change_key);
      }
      else{ //鎵惧埌鏍归噷鍘讳簡
        change_index = parent_page->ValueIndex(ori_page->GetPageId());
        parent_page->SetKeyAt(change_index, change_key);
      }
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }

  int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_, buffer_pool_manager_);
  bool didit;
  if(old_size != new_size ){ //鍒犻櫎鎴愬姛
    didit = CoalesceOrRedistribute(leaf_page, transaction);
  }
  //杩涘埌CoalesceOrRedistribute锛屽仛浜嗙殑璇濅細unpin锛屾病鍋氱殑璇濆氨涓嶄細
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if(didit) buffer_pool_manager_->DeletePage(leaf_page->GetPageId());

//鍒犻櫎DEBUG杈撳嚭鈥斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€斺€旓拷?
  /*
  ofstream outfile;
  outfile.open("bplustreegarph_delete.txt");
  auto *new_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  ToGraph(root_page, buffer_pool_manager_, outfile);
  ToString(root_page, buffer_pool_manager_);
  std::cout<<"瀹屾垚鍒犻櫎杈撳嚭"<<endl;
  */
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
//鎵惧埌鍏勫紵锛屽喅瀹氭槸merge杩樻槸redistribute銆傚鏋滆繖椤佃鍒犱簡灏辫繑鍥瀟rue锛屾病琚垹杩斿洖false
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  //妫€鏌ョ壒娈婃儏锟??
  if (node->IsRootPage()) { //鏄牴鑺傜偣鐨勮瘽鐩存帴璋冩暣锟??
    return AdjustRoot(node); 
  }
  else if (node->IsLeafPage()) { //鏄彾瀛愯妭鐐圭殑璇濓紝>=minsize鏃犻渶鎿嶄綔
    if (node->GetSize() >= node->GetMinSize()) {
      return false;
    }
  } 
  else { //鏄唴閮ㄨ妭鐐圭殑璇濓紝>minsize鏃犻渶鎿嶄綔            
    if (node->GetSize() > node->GetMinSize()) {
      return false;
    }
  }//缁忚繃鐩墠鐨勭瓫閫夛紝leaf_page涓€瀹氭槸宸茬粡澶皬锟??
  //鎯虫壘鍒板厔寮燂紝闇€瑕佸厛鎵惧埌鐖舵瘝
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw std::exception();
  }
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(page->GetData());
  //鎵惧埌杩欎釜鑺傜偣鍦ㄧ埗姣嶈妭鐐逛腑鐨刬ndex
  int node_index_in_parent = parent_node->ValueIndex(node->GetPageId());
  if(node_index_in_parent == parent_node->GetSize()){
    throw std::exception();
  }
  //纭畾鍏勫紵鑺傜偣鍦╬arent涓殑index锛屽線鍓嶅彇涓€涓€傚鏋滆嚜宸辨槸绗竴涓殑璇濓紝閭ｅ線鍚庡彇
  int sibling_page_id;
  if (node_index_in_parent == 0) {
    sibling_page_id = parent_node->ValueAt(1);
  } else {
    sibling_page_id = parent_node->ValueAt(node_index_in_parent - 1);
  }
  //鑾峰彇鍏勫紵鑺傜偣
  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr) {
    throw std::exception();
  }
  auto* sibling_node = reinterpret_cast<N *>(page->GetData()); //绫诲瀷鐢∟*

  //濡傛灉鍏勫紵鑺傜偣锛堝墠涓€涓妭鐐癸紝鎴栵拷?0锟??1涓妭鐐癸級姣旇緝澶э紝閭ｄ箞灏遍噸鍒嗛厤锛屽惁鍒欏氨merge
  bool to_coalesce = false;
  bool to_redistribute = false;
  bool this_node_deleted; //杩欎釜鍑芥暟鐨勮繑鍥烇拷?
  bool parent_deleted = false;
  if(sibling_node->GetSize() + node->GetSize() <= node->GetMaxSize()){//鍙互merge
    to_coalesce = true;
  }
  else{
    to_redistribute = true;
  }
  //寮€锟??
  if(to_redistribute == true){ //杩涜閲嶅垎锟??
    //杩欎釜鏄€夋嫨鍒伴噷闈㈠幓鍒ゆ柇
    Redistribute<N>(sibling_node, node, node_index_in_parent);  
    buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true); //鐬庡姞锟??
    return false;
  }
  else if(to_coalesce == true){ //杩涜merge
    if(node_index_in_parent == 0){ //浣嶄簬鏈€宸﹁竟锛岃鎹㈤偦锟??
      parent_deleted = Coalesce<N>(node, sibling_node, parent_node, 1, transaction);
      this_node_deleted = false; //铏界劧merge浼氬垹锛屼絾鏄繖閲岃皟鎹簡閭诲眳鍜屾湰韬紝瀹為檯鍙堟湪锟??
      //鍒犺嚜宸辨槸this_node_deleted锛屼笂闈alse浜嗗闈㈠氨涓嶅垹锟??
      buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(sibling_node->GetPageId());
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true); //鐬庡姞锟??
    }
    else { //姝ｅ父鎯呭喌锛屽線宸﹁竟
      parent_deleted = Coalesce<N>(sibling_node, node, parent_node, node_index_in_parent, transaction);
      buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true); //鐬庡姞锟??
      this_node_deleted = true;
    }
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  if(parent_deleted) buffer_pool_manager_->DeletePage(parent_node->GetPageId());//杩欒鏈夐锟??
  return this_node_deleted;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
//閫掑綊鍦板悎骞躲€傚鏋滅埗姣嶉〉闈㈣鍒狅紝杩斿洖true锛屽惁鍒欒繑鍥瀎alse
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&neighbor_node, N *&node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent, int index,
                              Transaction *transaction) {
  KeyType middle_key = parent->KeyAt(index);
  //鎼繃锟??
  node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);
  //鍦ㄧ埗姣嶉噷锟??
  parent->Remove(index);
  //閫掑綊鑰冭檻鐖舵瘝
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
//鎯冲乏鎴栧悜鍙抽噸鍒嗛厤锛屼笉閫掑綊
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  //杩欓噷娌℃湁缁檖arent锛屾壘middle_key杩樻湁鐐归夯锟??
  KeyType middle_key;
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw std::exception();
  }
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(page->GetData());
  middle_key = parent_node->KeyAt(index);

  if (index == 0) {
    //杩欓噷涓嶆槸鍔ㄨ嚜宸辫€屾槸鍔╪eighbor
    //neighbor_node绗竴涓妭鐐瑰彉鍖栦簡 瑕佸湪鐖舵瘝閲屽悓锟??
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    //浣嗘槸杩欎釜鍐呴儴鑺傜偣鏄湁鍦ㄥ仛锟??
    if(node->IsLeafPage()){
      auto *temp_page = buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
      InternalPage* neighbor_node_parent_node = reinterpret_cast<InternalPage *>(temp_page->GetData());
      int temp_index = neighbor_node_parent_node->ValueIndex(neighbor_node->GetPageId());
      neighbor_node_parent_node->SetKeyAt(temp_index,neighbor_node->KeyAt(0));
      buffer_pool_manager_->UnpinPage(neighbor_node_parent_node->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  } 
  else {
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
     //node绗竴涓妭鐐瑰彉鍖栦簡 瑕佸湪鐖舵瘝閲屽悓锟??
    if(node->IsLeafPage()){
      auto *temp_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
      InternalPage*node_parent_node = reinterpret_cast<InternalPage *>(temp_page->GetData());
      int temp_index = node_parent_node->ValueIndex(node->GetPageId());
      node_parent_node->SetKeyAt(temp_index,node->KeyAt(0));
      buffer_pool_manager_->UnpinPage(node_parent_node->GetPageId(), true);
    }
    //鍚庤皟琚尓鐨勫湪鐖舵瘝涓殑褰卞搷 杩欓噷涓嶇煡閬撴湁娌℃湁鎹嬫竻锟??
    parent_node->SetKeyAt(index, node->KeyAt(0)); 
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
//鏈€澶栧眰Remove鍒犻櫎leaf_page杩涘埌瑕佽皟鏁寸殑鍙戠幇鏄牴銆傛槸鏍圭殑璇濆熀鏈笉鐢ㄦ€庝箞璋冩暣鍏跺疄
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  //case1 涓嬮潰鍒犲畬鍙戠幇鍙湁涓€涓効瀛愪簡锛屾妸鍎垮瓙褰撴牴锟??
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto* internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = internal_node->RemoveAndReturnOnlyChild();
    auto* new_page = buffer_pool_manager_->FetchPage(new_root_page_id); //杩欓噷鍙栫殑灏辨槸鍎垮瓙
    auto* new_root_node = reinterpret_cast<InternalPage *>(new_page);
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    return true;
  }
  //case2 鍒犲畬锟??
  else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  //褰撳墠椤典负锟??
  if (IsEmpty()) {
    std::cout<<"璀﹀憡锛佸湪绌虹殑椤甸潰涓婂缓绔嬭凯浠ｅ櫒"<<endl;
    return INDEXITERATOR_TYPE(0, nullptr, buffer_pool_manager_);
  }
  //鎵惧埌鏈€宸﹁竟鐨勫彾锟??
  Page *page = FindLeafPage(KeyType{}, true);  // leftmost_leaf pinned
  LeafPage *leftmost_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(leftmost_leaf->GetPageId(), false);
  return INDEXITERATOR_TYPE(0, leftmost_leaf, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  //褰撳墠椤典负锟??
  if (IsEmpty()) {
    std::cout<<"璀﹀憡锛佸湪绌虹殑椤甸潰涓婂缓绔嬭凯浠ｅ櫒"<<endl;
    return INDEXITERATOR_TYPE(0, nullptr, buffer_pool_manager_);
  }
  //鎵惧埌鐩爣鐨勪綅锟??
  Page *page = FindLeafPage(key, true);  // leftmost_leaf pinned
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int start_index = leaf_page->KeyIndex(key, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(start_index, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  //鍏堟壘鍒版渶宸﹁竟
  Page *page = FindLeafPage(KeyType{}, true); 
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  //鐒跺悗涓嶆柇寰€鍚庡埌锟??
  while (leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false); //鍦ㄨ繖淇╄凯浠ｅ墠Unpin
    page = buffer_pool_manager_->FetchPage(next_page_id);  //杩唬杩欎咯
    leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false); 
  //杩斿洖鏈€锟?? 鏄繑鍥瀏etsize()锛岃繕鏄痝etsize()-1??????????
  int last_index = leaf_page->GetSize()-1;
  return INDEXITERATOR_TYPE(last_index, leaf_page, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
//缁欏畾key锛屾壘鍒板彾瀛愯妭锟??
//涓轰粈涔堣繑鍥炵被鍨嬫槸涓猵age*鑰屼笉鏄疊_PLUS_TREE_LEAF_PAGE_TYPE
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //std::cout<<"杩涘叆FindLeafPage"<<endl;

  //鎵惧埌鏍归〉锟??
  auto *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *cur_bptree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

  //涓嶆柇鍚戜笅鐩村埌鍙跺瓙
  while(cur_bptree_page->IsLeafPage() != true){
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_bptree_page);
    //纭畾涓嬩竴椤靛湪锟??
    page_id_t next_page_id;
    if(leftMost==true) next_page_id = internal_page->ValueAt(0);
    else next_page_id = internal_page->Lookup(key, comparator_);
    //杩唬鍓嶅線涓嬩竴锟??
    cur_page = buffer_pool_manager_->FetchPage(next_page_id);
    cur_bptree_page = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false); //涓嶇煡閬撹繖涔堝啓瀵逛笉锟??
  }
  //鍒拌揪鍙跺瓙椤甸潰锛岄€€鍑哄惊锟??
  return reinterpret_cast<Page *>(cur_bptree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  //鑾峰彇IndexRootsPage
  auto *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto *root_page = reinterpret_cast<IndexRootsPage *>(page->GetData()); //杩欎釜root_page鍜屼笂闈㈢殑涓嶄竴锟??
  //鏈潵鍙互鐩存帴鐢╢ind_page锛屼絾鏄痜ind_page鏄痯rivate锛屾墍浠ュ彧鑳界敤insert_record锟??
  if(insert_record == 1){ //鎻掑叆(鏂板缓)
    root_page->Insert(index_id_, root_page_id_);
  }
  else{ //鏇存柊
    root_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  //buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
