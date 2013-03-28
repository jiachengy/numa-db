#include <stdio.h> /* stdout */
#include <assert.h>
#include <pthread.h>
#include <string.h> /* strcpy */
#include <stdlib.h> /* malloc, free */

#include "perf.h"

#if defined(USE_PERF)

uint32_t nevents;

char *PERF_CONFIG;
char *PERF_OUT;

char *DEFAULT_EVENTS[] = {
  "PAPI_TOT_CYC",
  "PAPI_FP_INS"
};

int NUM_EVENTS = 0;
char **PERF_EVENT_NAMES;

static char *
mystrdup(const char *s)
{
  char *ss = (char*)malloc(strlen(s)+1);
  if (ss != NULL)
    memcpy(ss, s, strlen(s) + 1);

  return ss;
}

void
perf_lib_init(const char *perfcfg, const char *perfout)
{
  int retval;

  retval = PAPI_library_init(PAPI_VER_CURRENT);
  assert(retval == PAPI_VER_CURRENT);

  retval = PAPI_thread_init(pthread_self);
  assert(retval == PAPI_OK);

  if (perfcfg)
    PERF_CONFIG = mystrdup(perfcfg);

  if (perfout)
    PERF_OUT = mystrdup(perfout);

  if (PERF_CONFIG) {
    // parse and read customized event id
  }
  else { /* if no config file is specified, add default events only */
    NUM_EVENTS = sizeof(DEFAULT_EVENTS) / sizeof(char*);
    PERF_EVENT_NAMES = (char**)malloc(sizeof(char*) * NUM_EVENTS);
    memcpy(PERF_EVENT_NAMES, DEFAULT_EVENTS, sizeof(DEFAULT_EVENTS));
  }
}


void
perf_lib_cleanup()
{
  free(PERF_EVENT_NAMES);
  PAPI_shutdown();
}

void
perf_register_thread()
{
  PAPI_register_thread();
}

void
perf_unregister_thread()
{
  PAPI_unregister_thread();
}


void
perf_start(perf_t *perf)
{
  int retval = PAPI_start(perf->EventSet);
  assert(retval == PAPI_OK);
}

void
perf_stop(perf_t *perf)
{
  int retval = PAPI_stop(perf->EventSet, perf->values);
  assert(retval == PAPI_OK);
}

void
perf_print(perf_t *perf)
{
  for (int i = 0; i < NUM_EVENTS; i++)
    fprintf(stdout, "%s: %lld\n", PERF_EVENT_NAMES[i], perf->values[i]);
}


void
perf_aggregate(perf_t *total, perf_t *perf)
{
  for (int i = 0; i < NUM_EVENTS; i++)
    total->values[i] += perf->values[i];
}

perf_t*
perf_init()
{
  int retval;
  int EventSet = PAPI_NULL;

  retval = PAPI_create_eventset(&EventSet);
  assert(retval == PAPI_OK);

  retval = PAPI_assign_eventset_component(EventSet, 0);
  assert(retval == PAPI_OK);


  /* By default, the counter is inherited. */
  PAPI_option_t opt;
  memset(&opt, 0x0, sizeof(PAPI_option_t));
  
  opt.inherit.inherit = PAPI_INHERIT_ALL;
  opt.inherit.eventset = EventSet;

  retval = PAPI_set_opt(PAPI_INHERIT, &opt);
  assert(retval == PAPI_OK);

  /* Add events by names */
  for (int i = 0; i < NUM_EVENTS; i++) {
    int EventCode;
    retval = PAPI_event_name_to_code(PERF_EVENT_NAMES[i], &EventCode);
    assert(retval == PAPI_OK);

    retval = PAPI_query_event(EventCode);
    assert(retval == PAPI_OK);

    retval = PAPI_add_event(EventSet, EventCode);
    assert(retval == PAPI_OK);
  }

  perf_t *perf = (perf_t*)malloc(sizeof(perf_t));
  perf->EventSet = EventSet;
  perf->values = (counter_t*)malloc(sizeof(counter_t) * NUM_EVENTS);
  memset(perf->values, 0x0, sizeof(counter_t) * NUM_EVENTS);

  return perf;
}

void
perf_destroy(perf_t *perf)
{
  int retval;
  retval = PAPI_cleanup_eventset(perf->EventSet);
  assert(retval == PAPI_OK);

  retval = PAPI_destroy_eventset(&perf->EventSet);
  assert(retval == PAPI_OK);

  free(perf);
}




#endif
