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
  /*
  if (!(*it)->empty()) {
    pthread_mutex_unlock(&mutex_);
    return (*it)->FetchAtomic();
  }

  do {
    // if empty and is ready to retire
    if ((*it)->exhausted()) {
      it = actives_.erase(it);
    }
    if (!(*it)->empty()) {
      Tasklist * nonempty = *it;
      if (it != actives_.begin()) { // it not at beginning, promote it
        actives_.erase(it);
        actives_.push_front(nonempty);
      }
      pthread_mutex_unlock(&mutex_);
      return nonempty->FetchAtomic();
    }

    ++it;

  } while (it != actives_.end());
  */

  while (it != actives_.end()) {
    // if not empty
    if (!(*it)->empty()) {
      Tasklist * nonempty = *it;
      if (it != actives_.begin()) { // it not at beginning, promote it
        actives_.erase(it);
        actives_.push_front(nonempty);
      }
      pthread_mutex_unlock(&mutex_);
      return nonempty->FetchAtomic();
    }

    // if empty and is ready to retire
    if ((*it)->exhausted()) {
      it = actives_.erase(it);
    }
    else {
      ++it;
    }
  }
  pthread_mutex_unlock(&mutex_);
  return NULL;
}



