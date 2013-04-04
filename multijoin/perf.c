#include <stdio.h> /* stdout */
#include <assert.h>
#include <pthread.h>
#include <string.h> /* strcpy */
#include <stdlib.h> /* malloc, free */

#include "perf.h"

#if defined(USE_PERF)

char *PERF_CONFIG;
char *PERF_OUT;

char const *DEFAULT_EVENTS[] = {
  "PAPI_TOT_CYC", /* total cpu cycles */
    "PAPI_TOT_INS", /* total instructions completed */
  //  "PAPI_L1_ICM",
   /* "PAPI_L1_DCM", */
   /*   "PAPI_L2_DCM", */
    "PAPI_L3_TCM", /* Data TLB misses */
    "PAPI_TLB_DM", /* Data TLB misses */
  //    "PAPI_BR_MSP", /* conditional branch mispredicted */
};

int NUM_EVENTS = 0;
char **PERF_EVENT_NAMES;
perf_counter_t PERF_COUNTER_INITIALIZER;

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

  int max_counters = PAPI_num_counters();
  if (max_counters > MAX_PERF_EVENTS)
    max_counters = MAX_PERF_EVENTS;

  PERF_EVENT_NAMES = (char**)malloc(sizeof(char*) * max_counters);  
  assert(PERF_EVENT_NAMES != NULL);
  memset(PERF_EVENT_NAMES, 0x0, sizeof(char*) * max_counters);

  if (PERF_CONFIG) {
    char line[80];
    FILE *config = fopen(PERF_CONFIG, "r");
    assert(config != NULL);

    while (fgets(line, 80, config) != NULL && NUM_EVENTS < max_counters) {
      if (line[0]=='#')
        continue;
      PERF_EVENT_NAMES[NUM_EVENTS] = mystrdup(line);
      NUM_EVENTS++;
    }
    if (!feof(config))
      fprintf(stderr, "Too many counters added. Only take the first %d.\n", max_counters);

    fclose(config);
  }
  else { /* if no config file is specified, add default events only */
    NUM_EVENTS = sizeof(DEFAULT_EVENTS) / sizeof(char*);
    if (NUM_EVENTS > max_counters) {
      NUM_EVENTS = max_counters;
      fprintf(stderr, "Too many counters added. Only take the first %d.\n", max_counters);
    }
    
    for (int i = 0; i < NUM_EVENTS; i++)
      PERF_EVENT_NAMES[i] = mystrdup(DEFAULT_EVENTS[i]);
  }

  for (int i = 0; i != NUM_EVENTS; ++i)
    PERF_COUNTER_INITIALIZER.value[i] = 0;
}


void
perf_lib_cleanup()
{
  for (int i = 0; i < NUM_EVENTS; i++)
    free(PERF_EVENT_NAMES[i]);
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

/* void */
/* perf_reset(perf_t *perf) */
/* { */
/*   int retval = PAPI_reset(perf->EventSet); */
/*   assert(retval == PAPI_OK); */
/* } */

void
perf_stop(perf_t *perf)
{
  int retval = PAPI_stop(perf->EventSet, NULL);
  assert(retval == PAPI_OK);
}

void
perf_print(perf_counter_t counter)
{
  FILE *out;

  if (PERF_OUT) {
    out = fopen(PERF_OUT, "w+");
    assert(out != NULL);
  }
  else
    out = stdout;

  for (int i = 0; i < NUM_EVENTS; i++)
    fprintf(out, "%s: %lld\n", PERF_EVENT_NAMES[i], counter.value[i]);
}

/* void */
/* perf_accum(perf_t *perf) */
/* { */
/*   int retval = PAPI_accum(perf->EventSet, perf->values); */
/*   assert(retval == PAPI_OK); */
/* } */

perf_counter_t
perf_read(perf_t *perf)
{
  perf_counter_t counter;
  counter.tick = PAPI_get_real_usec();
  int retval = PAPI_read(perf->EventSet, (counter_t*)&counter.value);
  assert(retval == PAPI_OK);
  return counter;
}


/* void */
/* perf_aggregate(perf_t *total, perf_t *perf) */
/* { */
/*   for (int i = 0; i < NUM_EVENTS; i++) */
/*     total->values[i] += perf->values[i]; */
/* } */

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
    if ((retval = PAPI_event_name_to_code(PERF_EVENT_NAMES[i], &EventCode)) != PAPI_OK) {
      fprintf(stderr, "Event name %s not found, skipped.\n", PERF_EVENT_NAMES[i]);
      continue;
    }


    if ((retval = PAPI_query_event(EventCode)) != PAPI_OK) {
      fprintf(stderr, "Event %s not supported on this hardware, skipped.\n", PERF_EVENT_NAMES[i]);
      continue;
    }

    if ((retval = PAPI_add_event(EventSet, EventCode)) != PAPI_OK) {
      assert(retval == PAPI_ECNFLCT);
      fprintf(stderr, "%s conflicts, skipped.\n", PERF_EVENT_NAMES[i]);
    }
  }

  perf_t *perf = (perf_t*)malloc(sizeof(perf_t));
  perf->EventSet = EventSet;
  /* perf->values = (counter_t*)malloc(sizeof(counter_t) * NUM_EVENTS); */
  /* memset(perf->values, 0x0, sizeof(counter_t) * NUM_EVENTS); */

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

void
perf_counter_aggr(perf_counter_t *lhs, perf_counter_t rhs)
{
  for (int i = 0; i != NUM_EVENTS; ++i)
    lhs->value[i] += rhs.value[i];
  lhs->tick += rhs.tick;
}

perf_counter_t perf_counter_diff(perf_counter_t before, perf_counter_t after)
{
  perf_counter_t result;
  result.tick = after.tick - before.tick;
  for (int i = 0; i != NUM_EVENTS; ++i)
    result.value[i] = after.value[i] - before.value[i];
  return result;
}


#endif
