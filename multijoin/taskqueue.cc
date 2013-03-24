#include "taskqueue.h"

Task* Tasklist::Fetch()
{
  if (tasks_.empty())
    return NULL;
  Task* next = tasks_.front();
  tasks_.pop();
  return next;
}

Task* Tasklist::FetchAtomic()
{
  pthread_mutex_lock(&mutex_);
  if (tasks_.empty()) {
    pthread_mutex_unlock(&mutex_);
    return NULL;
  }
  Task* next = tasks_.front();
  tasks_.pop();
  pthread_mutex_unlock(&mutex_);
  return next;
}


Task* Taskqueue::Fetch()
{
  pthread_mutex_lock(&mutex_);
  list<Tasklist*>::iterator it = actives_.begin();
  while (it != actives_.end()) {
    // if not empty
    if (!(*it)->empty()) {
      pthread_mutex_unlock(&mutex_);
      return (*it)->FetchAtomic();
    }

    // if empty and is ready to retire
    if ((*it)->in()->ready())
      it = actives_.erase(it);
    else
      ++it;
  }
  pthread_mutex_unlock(&mutex_);
  return NULL;
}



