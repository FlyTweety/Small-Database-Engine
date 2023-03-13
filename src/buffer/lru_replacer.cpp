#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_pages = num_pages; //lru_list中最大的页数
  //lru_list.clear(); //初始化清�?
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { //替换（即删除）与所有被跟踪的页相比最近最少被访问的页，将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中输出并返回true，如果当前没有可以替换的元素则返回false�?
  if(!lru_list.empty()){ //如果非空说明能删，那就删掉第一�?
    *frame_id = lru_list[0];
    lru_list.erase(lru_list.begin(), lru_list.begin() + 1);
    return true;
  }
  else return false; //vector是空的，没有能删�?
}

void LRUReplacer::Pin(frame_id_t frame_id) { //将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧。Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用�?
  for(size_t i = 0; i < lru_list.size(); i++){ //遍历lru_list，找到后删掉
    if(lru_list[i] == frame_id){
      lru_list.erase(lru_list.begin() + i, lru_list.begin() + i + 1);
      break;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) { //将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。Unpin函数应当在一个数据页的引用计数变�?0时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换；
  //先检测原来已有的话就不做�? 这里其实不符合LRU 按理说应该删了原来的然后重新插入 但是TEST是这样的
  int flag = 0;
  for(size_t i = 0; i < lru_list.size(); i++){
    if(lru_list[i] == frame_id){
      flag = 1;
      break;
    }
  }
  if(flag) ; //如果原来有，就不做了
  else if(lru_list.size() == max_pages){ //原来没有，可以操作。但是lru_list满了，要�?
    int value; //用来调用Victim
    this->Victim(&value); //删掉第一�?
    lru_list.push_back(frame_id); //插入这个
  }
  else{ //直接�?
    lru_list.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() { //此方法返回当前LRUReplacer中能够被替换的数据页的数量�?
  return lru_list.size(); //直接返回vector的size就行
}
