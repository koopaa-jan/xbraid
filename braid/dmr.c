#include "dmr.h"

void DMR_Send_shrink(double *data, int size, MPI_Comm DMR_INTERCOMM)
{
    // printf("(sergio): %s(%s,%d)\n", __FILE__, __func__, __LINE__);
    int my_rank, comm_size, intercomm_size, factor, dst;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_remote_size(DMR_INTERCOMM, &intercomm_size);
    factor = comm_size / intercomm_size;
    dst = my_rank / factor;
    MPI_Send(data, size, MPI_DOUBLE, dst, 0, DMR_INTERCOMM);
}

void DMR_Recv_shrink(double **data, int *size, MPI_Comm DMR_INTERCOMM)
{
    // printf("(sergio): %s(%s,%d)\n", __FILE__, __func__, __LINE__);
    int my_rank, comm_size, parent_size, src, factor, iniPart, i, parent_data_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_remote_size(DMR_INTERCOMM, &parent_size);
    MPI_Status status;
    factor = parent_size / comm_size;
    *data = (double *)malloc((*size) * sizeof(double));
    for (i = 0; i < factor; i++)
    {
        src = my_rank * factor + i;
        iniPart = parent_data_size * i;
        MPI_Recv((*data) + iniPart, (*size), MPI_DOUBLE, src, 0, DMR_INTERCOMM, &status);
    }
}

void DMR_Send_expand(double *data, int size, MPI_Comm DMR_INTERCOMM)
{
    // printf("(sergio): %s(%s,%d): %d\n", __FILE__, __func__, __LINE__, size);
    int my_rank, comm_size, intercomm_size, localSize, factor, dst;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_remote_size(DMR_INTERCOMM, &intercomm_size);
    factor = intercomm_size / comm_size;
    localSize = size / factor;
    for (int i = 0; i < factor; i++)
    {
        dst = my_rank * factor + i;
        int iniPart = localSize * i;
        // printf("(sergio): %s(%s,%d): %d send to %d (%d)\n", __FILE__, __func__, __LINE__, my_rank, dst, localSize);
        MPI_Send(data + iniPart, localSize, MPI_DOUBLE, dst, 0, DMR_INTERCOMM);
    }
}

void DMR_Recv_expand(double **data, int *size, MPI_Comm DMR_INTERCOMM)
{
    // printf("(sergio): %s(%s,%d): %d\n", __FILE__, __func__, __LINE__, *size);
    int my_rank, comm_size, parent_size, src, factor;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
    MPI_Comm_remote_size(DMR_INTERCOMM, &parent_size);
    MPI_Status status;
    factor = comm_size / parent_size;
    *data = (double *)malloc((*size) * sizeof(double));
    src = my_rank / factor;
    // printf("(sergio): %s(%s,%d): %d recv from %d (%d)\n", __FILE__, __func__, __LINE__, my_rank, src, *size);
    MPI_Recv(*data, *size, MPI_DOUBLE, src, 0, DMR_INTERCOMM, &status);
}


void DMR_Set_parameters(MPI_Info mpi_info){
    info = mpi_info;
}