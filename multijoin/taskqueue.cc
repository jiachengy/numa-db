#include "taskqueue.h"

Task* Tasklist::Fetch() {
  if (tasks_.empty())
    return NULL;
  Task* next = tasks_.front();
  tasks_.pop_front();
  return next;
}

Task* Taskqueue::Fetch() {
  list<Tasklist*>::iterator it = actives_.begin();
  while (it != actives_.end()) {
    if ((*it)->in()->done()) { // if the tasklist is REALLY finished. retire it
      actives_.erase(it++);
    }
    else {
      Task *task = (*it)->Fetch();
      if (task)
        return task;
      ++it;
    }
  }
  return NULL;
}

