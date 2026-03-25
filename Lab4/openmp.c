#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <omp.h>
#include <stdbool.h>

#define RANGE_SIZE 1000

bool is_prime(long num) {
  if (num == 0 || num == 1) return false;
  if (num == 2) return true;

  for (long i = 2; i*i <= num; i++) {
    if (num % i == 0) {
      return false;
    }
  }
  return true;
}

long count_twin_primes_in_range(long start, long end, long hard_limit) {
  if (start%2 == 0) start++;
  if (end > hard_limit) end = hard_limit;

  long twin_primes = 0;
  bool last_was_prime = false;
  for (long i=start; i < end; i+=2) {
    if (is_prime(i)) {
      if (last_was_prime) {
        twin_primes++;
      }
      last_was_prime = true;
    } else {
      last_was_prime = false;
    }
  }

  if (last_was_prime) {
    long next_potential_prime = end+1 + (end%2 == 0 ? 0 : 1);
    if (next_potential_prime <= hard_limit && is_prime(next_potential_prime)) {
      twin_primes++;
    }
  }

  return twin_primes;
}


int main(int argc,char **argv) {
  
  Args ins__args;
  parseArgs(&ins__args, &argc, argv);

  //set number of threads
  omp_set_num_threads(ins__args.n_thr);
  
  //program input argument
  long inputArgument = ins__args.arg; 

  struct timeval ins__tstart, ins__tstop;
  gettimeofday(&ins__tstart, NULL);
  
  // run your computations here (including OpenMP stuff)
  int numOfChunks = (inputArgument + RANGE_SIZE - 1) / RANGE_SIZE;
  long totalTwinPrimes = 0;
  long startRange, endRange;

  #pragma omp parallel for private(startRange, endRange) reduction(+:totalTwinPrimes)
  for (int i = 0; i < numOfChunks; i++) {
    startRange = i * RANGE_SIZE;
    endRange = startRange + RANGE_SIZE;
    if (endRange > inputArgument) {
      endRange = inputArgument;
    }
    totalTwinPrimes += count_twin_primes_in_range(startRange, endRange, inputArgument);
  }
  printf("Total twin primes up to %ld: %ld\n", inputArgument, totalTwinPrimes);

  // synchronize/finalize your computations
  gettimeofday(&ins__tstop, NULL);
  ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
}
