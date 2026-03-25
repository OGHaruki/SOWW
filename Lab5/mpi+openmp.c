#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <mpi.h>
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

void calculate(int rank, long globalStart, long globalEnd, long limit) {
  long localResult = 0;
  long globalResult = 0;

  #pragma omp parallel for private(localResult) reduction(+:globalResult)
  for (int i = globalStart; i < globalEnd; i += RANGE_SIZE) {
    long rangeEnd = i + RANGE_SIZE;
    if (rangeEnd > globalEnd) {
      rangeEnd = globalEnd;
    }
    localResult = count_twin_primes_in_range(i, rangeEnd, limit);
    globalResult += localResult;
  }
  MPI_Send(&globalResult, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
}

int main(int argc,char **argv) {

  Args ins__args;
  parseArgs(&ins__args, &argc, argv);

  //set number of threads
  omp_set_num_threads(ins__args.n_thr);
  
  //program input argument
  long inputArgument = ins__args.arg; 

  struct timeval ins__tstart, ins__tstop;

  int threadsupport;
  int myrank,nproc;
  // Initialize MPI with desired support for multithreading -- state your desired support level

  MPI_Init_thread(&argc, &argv,MPI_THREAD_FUNNELED,&threadsupport); 

  if (threadsupport<MPI_THREAD_FUNNELED) {
    printf("\nThe implementation does not support MPI_THREAD_FUNNELED, it supports level %d\n",threadsupport);
    MPI_Finalize();
    return -1;
  }
  
  // obtain my rank
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  // and the number of processes
  MPI_Comm_size(MPI_COMM_WORLD,&nproc);

  if(!myrank){
      gettimeofday(&ins__tstart, NULL);
  }
  // run your computations here (including MPI communication and OpenMP stuff)
  int step = inputArgument / nproc;
  long start = myrank * step;
  long end = 0;
  if (myrank == nproc - 1) {
    end = inputArgument;
  } else {
    end = start + step;
  }
  calculate(myrank, start, end, inputArgument);

  if (!myrank) {
    long resulttemp;
    long finalResult = 0;
    for (int i = 0; i < nproc; i++) {
      MPI_Recv(&resulttemp, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      finalResult += resulttemp;
    }
    printf("Number of twin primes up to %ld: %ld\n", inputArgument, finalResult);
  }

  // synchronize/finalize your computations

  if (!myrank) {
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  }
    
  MPI_Finalize();
  
}
