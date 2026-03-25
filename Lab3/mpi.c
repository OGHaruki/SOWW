#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <mpi.h>
#include "numgen.c"
#include <stdbool.h>

#define MASTER_RANK 0
#define RANGE_SIZE 1000
#define DATA 0
#define RESULT 1

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

void sendRange(int rank, long range[2])
{
  printf ("\nMaster sending range %ld,%ld to process %d", range[0], range[1], rank);;
  MPI_Send(range, 2, MPI_LONG, rank, DATA, MPI_COMM_WORLD);
}

typedef struct {
  long range[2];
  long result;
} SlaveData;

void master(long arraySize, int nproc) {
  MPI_Request *requests = (MPI_Request *) malloc ((nproc - 1) * sizeof (MPI_Request));
  if (!requests)
	{
    printf ("\nNot enough memory");
    MPI_Finalize ();
    return;
	}

  SlaveData *slaveData = (SlaveData *)malloc((nproc - 1) * sizeof(SlaveData));
  if (!slaveData)
  {
    printf("\nNot enough memory");
    MPI_Finalize();
    return;
  }

  long range[2] = {1, 0};
  long result = 0;
  int workingSlaves = 0;

  for (int i = 1; i <= nproc - 1; i++) {
    // check if the start of the range is on edge of the array
    if (range[0] >= arraySize) {
      long finish_signal[2] = {0, 0};
      printf("Shutting down process %d\n", i);
      MPI_Send(finish_signal, 2, MPI_LONG, i, DATA, MPI_COMM_WORLD);
      continue;
    }
    range[1] = range[0] + RANGE_SIZE - 1;
    if (range[1] > arraySize) {
      range[1] = arraySize;
    }

    slaveData[i-1].range[0] = range[0];
    slaveData[i-1].range[1] = range[1];
    
    MPI_Send(slaveData[i-1].range, 2, MPI_LONG, i, DATA, MPI_COMM_WORLD);
    printf("Sent range %ld,%ld to process %d\n", range[0], range[1], i);
    workingSlaves++;
    
    range[0] = range[1] + 1;
  }

  for (int i = workingSlaves + 1; i <= nproc - 1; i++) {
    long finish_signal[2] = {0, 0};
    printf("Shutting down process %d\n", i);
    MPI_Send(finish_signal, 2, MPI_LONG, i, DATA, MPI_COMM_WORLD);
  }

  for (int i = 0; i < workingSlaves; i++) {
    requests[i] = MPI_REQUEST_NULL;
  }

  for (int i = 1; i <= workingSlaves; i++) {
    MPI_Irecv(&slaveData[i-1].result, 1, MPI_LONG, i, RESULT, MPI_COMM_WORLD, &requests[i-1]);
  }

  while (range[0] <= arraySize) {
    int requestCompleted;

    MPI_Waitany(workingSlaves, requests, &requestCompleted, MPI_STATUS_IGNORE);
    printf("Received result %ld from process %d\n", slaveData[requestCompleted].result, requestCompleted + 1);
    result += slaveData[requestCompleted].result;
    
    range[1] = range[0] + RANGE_SIZE - 1;
    if (range[1] > arraySize) {
      range[1] = arraySize;
    }

    slaveData[requestCompleted].range[0] = range[0];
    slaveData[requestCompleted].range[1] = range[1];

    MPI_Send(slaveData[requestCompleted].range, 2, MPI_LONG, requestCompleted + 1, DATA, MPI_COMM_WORLD);
    printf("Sent range %ld,%ld to process %d\n", range[0], range[1], requestCompleted + 1);

    MPI_Irecv(&slaveData[requestCompleted].result, 1, MPI_LONG, requestCompleted + 1, RESULT, MPI_COMM_WORLD, &requests[requestCompleted]);
    printf("Received result %ld from process %d\n", slaveData[requestCompleted].result,requestCompleted + 1);
    
    range[0] = range[1] + 1;
  }

  for (int i = 0; i < workingSlaves; i++) {
    int requestCompleted;

    MPI_Waitany(workingSlaves, requests, &requestCompleted, MPI_STATUS_IGNORE);
    printf("Received result %ld from process %d\n", slaveData[requestCompleted].result, requestCompleted + 1);
    result += slaveData[requestCompleted].result;

    long finish_signal[2] = {0, 0};
    MPI_Send(finish_signal, 2, MPI_LONG, requestCompleted + 1, DATA, MPI_COMM_WORLD);
    printf("Shutting down process %d\n", requestCompleted + 1);
  }

  printf("\nTotal twin primes: %ld\n", result);
  free(requests);
  free(slaveData);
}

void slave(long arraySize, int rank) {
  MPI_Status status;
  long currentRange[2];
  long currentResult;

  MPI_Recv(currentRange, 2, MPI_LONG, MASTER_RANK, DATA, MPI_COMM_WORLD, &status);
  printf("Process %d received range %ld,%ld\n", rank, currentRange[0], currentRange[1]);

  while (currentRange[0] != 0) {
    long currentResult = count_twin_primes_in_range(currentRange[0], currentRange[1], arraySize);
    MPI_Request sendRequest = MPI_REQUEST_NULL;
    MPI_Isend(&currentResult, 1, MPI_LONG, MASTER_RANK, RESULT, MPI_COMM_WORLD, &sendRequest);

    MPI_Recv(currentRange, 2, MPI_LONG, MASTER_RANK, DATA, MPI_COMM_WORLD, &status);
    printf("Process %d received range %ld,%ld\n", rank, currentRange[0], currentRange[1]);
    MPI_Wait(&sendRequest, MPI_STATUS_IGNORE);
  }
}

int main(int argc,char **argv) {

  Args ins__args;
  parseArgs(&ins__args, &argc, argv);

  //program input argument
  long inputArgument = ins__args.arg; 

  struct timeval ins__tstart, ins__tstop;

  int myrank,nproc;
  unsigned long int *numbers;

  MPI_Init(&argc,&argv);

  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  MPI_Comm_size(MPI_COMM_WORLD,&nproc);

  if (myrank == MASTER_RANK) {
    gettimeofday(&ins__tstart, NULL);
    master(inputArgument, nproc);
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  } else {
    slave(inputArgument, myrank);
  }
  
  MPI_Finalize();

}
