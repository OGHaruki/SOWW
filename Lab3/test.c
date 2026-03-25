#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <mpi.h>
#include <stdbool.h>

#define RANGE_SIZE 1000
#define TAG_RANGE 0
#define TAG_RESULT 1

bool isPrime(long n)
{
  if (n <= 1)
    return false;
  if (n == 2)
    return true;
  if (n % 2 == 0)
    return false;
  for (long i = 3; i * i <= n; i += 2)
    if (n % i == 0)
      return false;
  return true;
}

long countTwinPrimes(long start, long end, long limit)
{
  long count = 0;
  if (start % 2 == 0)
    start++;
  bool prevIsPrime = false;
  for (long i = start; i < end; i += 2)
  {
    if (isPrime(i))
    {
      if (prevIsPrime)
        count++;
      prevIsPrime = true;
    }
    else
    {
      prevIsPrime = false;
    }
  }

  if (prevIsPrime)
  {
    if (end % 2 == 0)
      end++;
    if (end <= limit && isPrime(end))
      count++;
  }
  return count;
}

#pragma region Master
void sendRange(int rank, long range[2])
{
  printf("Sending range [%ld, %ld] to process %d\n", range[0], range[1], rank);
  MPI_Send(range, 2, MPI_LONG, rank, TAG_RANGE, MPI_COMM_WORLD);
}

void shutdownSlave(int rank)
{
  printf("Shutting down process %d\n", rank);
  sendRange(rank, (long[]){0, 0});
}

void sendRangeAsync(int rank, long range[2], MPI_Request *request)
{
  printf("Asynchronously sending range [%ld, %ld] to process %d\n", range[0], range[1], rank);
  MPI_Isend(range, 2, MPI_LONG, rank, TAG_RANGE, MPI_COMM_WORLD, request);
}

void receiveResultAsync(int rank, long *result, MPI_Request *request)
{
  MPI_Irecv(result, 1, MPI_LONG, rank, TAG_RESULT, MPI_COMM_WORLD, request);
  printf("Asynchronously receiving result from process %d\n", rank);
}

void waitForRequest(MPI_Request *request)
{
  MPI_Wait(request, MPI_STATUS_IGNORE);
}

void waitForAny(int count, MPI_Request *requests, int *completed)
{
  MPI_Waitany(count, requests, completed, MPI_STATUS_IGNORE);
}

void waitForAll(int count, MPI_Request *requests)
{
  MPI_Waitall(count, requests, MPI_STATUSES_IGNORE);
}

void testAny(int count, MPI_Request *requests, int *completed, int *flag)
{
  MPI_Testany(count, requests, completed, flag, MPI_STATUS_IGNORE);
}
#pragma endregion

#pragma region Slave
void receiveRange(long range[2], MPI_Status *status)
{
  MPI_Recv(range, 2, MPI_LONG, 0, TAG_RANGE, MPI_COMM_WORLD, status);
}

void receiveRangeAsync(long range[2], MPI_Request *request)
{
  MPI_Irecv(range, 2, MPI_LONG, 0, TAG_RANGE, MPI_COMM_WORLD, request);
}

void sendResultAsync(long result, MPI_Request *request)
{
  MPI_Isend(&result, 1, MPI_LONG, 0, TAG_RESULT, MPI_COMM_WORLD, request);
}
#pragma endregion

typedef struct
{
  long range[2];
  long result;
} SlaveData;

void master(long arraySize, int procCount)
{
  MPI_Request *requests = (MPI_Request *)malloc((procCount - 1) * sizeof(MPI_Request));
  if (!requests)
  {
    printf("\nNot enough memory");
    MPI_Finalize();
    return;
  }

  SlaveData *slaveData = (SlaveData *)malloc((procCount - 1) * sizeof(SlaveData));
  if (!slaveData)
  {
    printf("\nNot enough memory");
    free(requests);
    MPI_Finalize();
    return;
  }

  long range[2] = {1, 0};
  long result = 0;
  int activeSlaves = procCount - 1;

  for (int i = 1; i <= procCount - 1; i++)
  {
    if (range[0] >= arraySize)
    {
      shutdownSlave(i);
      activeSlaves--;
      continue;
    }
    range[1] = range[0] + RANGE_SIZE;
    if (range[1] > arraySize)
      range[1] = arraySize;
    sendRange(i, range);
    range[0] = range[1];
  }

  for (int i = 0; i < activeSlaves; i++)
    requests[i] = MPI_REQUEST_NULL;

  for (int i = 1; i <= activeSlaves; i++)
    receiveResultAsync(i, &(slaveData[i - 1].result), &(requests[i - 1]));

  while (range[1] < arraySize)
  {
    int requestCompleted, flag;
    testAny(activeSlaves, requests, &requestCompleted, &flag);
    if (!flag)
    {
      // do some work while waiting for a request to complete
      range[1] = range[0] + 10;
      if (range[1] > arraySize)
        range[1] = arraySize;
      long tempResult = countTwinPrimes(range[0], range[1], arraySize);
      result += tempResult;
      range[0] = range[1];
      continue;
    }

    result += slaveData[requestCompleted].result;

    range[1] = range[0] + RANGE_SIZE;
    if (range[1] > arraySize)
      range[1] = arraySize;

    slaveData[requestCompleted].range[0] = range[0];
    slaveData[requestCompleted].range[1] = range[1];
    sendRangeAsync(requestCompleted + 1, slaveData[requestCompleted].range, &(requests[requestCompleted]));

    range[0] = range[1];
    receiveResultAsync(requestCompleted + 1, &(slaveData[requestCompleted].result), &(requests[requestCompleted]));
  }

  for (int i = 0; i < activeSlaves; i++)
  {
    int requestCompleted;
    waitForAny(activeSlaves, requests, &requestCompleted);
    result += slaveData[requestCompleted].result;
    shutdownSlave(requestCompleted + 1);
  }

  free(requests);
  free(slaveData);

  printf("Twin primes up to %ld: %ld\n", arraySize, result);
}

void slave(long arraySize, int rank)
{
  MPI_Status receiveStatus;
  MPI_Request receiveRequest = MPI_REQUEST_NULL, sendRequest = MPI_REQUEST_NULL;
  long range[2];

  receiveRange(range, &receiveStatus);

  while (range[0] != range[1])
  {
    long nextRange[2];
    receiveRangeAsync(nextRange, &receiveRequest);

    long result = countTwinPrimes(range[0], range[1], arraySize);
    sendResultAsync(result, &sendRequest);

    waitForRequest(&receiveRequest);
    range[0] = nextRange[0];
    range[1] = nextRange[1];
  }

  waitForRequest(&sendRequest);
}

int main(int argc, char **argv)
{
  Args ins__args;
  parseArgs(&ins__args, &argc, argv);
  long inputArgument = ins__args.arg;

  struct timeval ins__tstart, ins__tstop;
  int rank, procCount;

  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &procCount);

  if (rank == 0)
  {
    gettimeofday(&ins__tstart, NULL);
    master(inputArgument, procCount);
    gettimeofday(&ins__tstop, NULL);
    ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  }
  else
  {
    slave(inputArgument, rank);
  }

  MPI_Finalize();
}