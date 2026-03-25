#include "utility.h"
#include <stdbool.h>
#include <stdio.h>
#include <sys/time.h>
#include <mpi.h>

const int MASTER_RANK = 0;

#define RANGESIZE 1000
#define DATA 0
#define RESULT 1
#define FINISH 2


const int REQUEST_RANGE_SIZE = 1000;

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
  for (long i=start; i <= end; i+=2) {
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

long master(int nproc, long start, long end) {
  MPI_Status status;
  long result = 0, resulttemp;
  long range[2];
  int i;

  range[0] = start;

  for (i = 1; i < nproc; i++) {
    range[1] = range[0] + RANGESIZE;
    if (range[1] > end) range[1] = end;
    
    MPI_Send(range, 2, MPI_LONG, i, DATA, MPI_COMM_WORLD);
    range[0] = range[1] + 1
  }

  do {
    MPI_Recv(&resulttemp, 1, MPI_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &status);
    result += resulttemp;

    range[1] = range[0] + RANGESIZE;
    if (range[1] > end) range[1] = end;

    MPI_Send(range, 2, MPI_LONG, status.MPI_SOURCE, DATA, MPI_COMM_WORLD);
    range[0] = range[1] + 1;
  } while (range[0] <= end);

  for (i = 1; i < nproc; i++) {
    MPI_Recv(&resulttemp, 1, MPI_LONG, MPI_ANY_SOURCE, RESULT, MPI_COMM_WORLD, &status);
    result += resulttemp;
  }

  for (i = 1; i < nproc; i++) {
    MPI_Send(NULL, 0, MPI_LONG, i, FINISH, MPI_COMM_WORLD);
  }

  return result;
}

void slave(long hard_limit) {
  MPI_Status status;
  long range[2];
  long resulttemp;

  do {
    MPI_Probe(MASTER_RANK, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    if (status.MPI_TAG == DATA) {
      MPI_Recv(range, 2, MPI_LONG, MASTER_RANK, DATA, MPI_COMM_WORLD, &status);
      resulttemp = count_twin_primes_in_range(range[0], range[1], hard_limit);
      MPI_Send(&resulttemp, 1, MPI_LONG, MASTER_RANK, RESULT, MPI_COMM_WORLD);
    }
  } while (status.MPI_TAG != FINISH);
}


int main(int argc,char **argv) {
  Args ins__args;
  parseArgs(&ins__args, &argc, argv);

  const int START_NUMBER = ins__args.start; 
  const int END_NUMBER = ins__args.stop;
  struct timeval ins__tstart, ins__tstop;

  int myrank,nproc;
  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nproc);

  if (myrank == MASTER_RANK) {
    gettimeofday(&ins__tstart, NULL);
  }

  // run your computations here (including MPI communication)

  long twin_primes = 0;

  if (myrank == MASTER_RANK) {
    twin_primes = master(nproc, START_NUMBER, END_NUMBER);
  } else {
    slave(END_NUMBER);
  }

  // synchronize/finalize your computations

  if (myrank == MASTER_RANK) {
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);

    printf("result=%ld\n", twin_primes);
  }
  
  MPI_Finalize();
}