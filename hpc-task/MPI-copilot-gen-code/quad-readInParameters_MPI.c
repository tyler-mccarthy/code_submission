/*
 * MPI-based implementation of the trapezoidal rule for numerical integration.
 * Optimized for 250 million trapezoids with domain decomposition and collective communication.
 * Features: rolling endpoint calculations, local scalar accumulators, arithmetic optimizations.
 *
 * (c) michael k bane, tyler mccarthy
 * v1.0 (23Apr2025)
 */

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "func_utils.h"

double func(double x); // Prototype for the function to integrate (separate file)

int main(int argc, char* argv[]) {
    double a, b;  // Integration bounds
    long long numberQuads;
    double width, halfW;
    double wallStart, wallEnd; // added to track time
    double local_integral = 0.0, global_integral = 0.0;

    int rank, size;

    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        wallStart = MPI_Wtime();
        // Read input parameters on rank 0
        if (argc < 4) {
            printf("Need to pass bounds ('a','b') and numberQuads\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        a = atof(argv[1]);
        b = atof(argv[2]);
        numberQuads = atoll(argv[3]);
        printf("Integrating from a=%f to b=%f using %lld trapezoids\n", a, b, numberQuads);
    }

    // Broadcast input parameters to all ranks
    MPI_Bcast(&a, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&b, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&numberQuads, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    width = (b - a) / (double)numberQuads;
    halfW = width / 2.0;

    // Domain decomposition: calculate local range for each rank
    long long local_start = rank * (numberQuads / size);
    long long local_end = (rank == size - 1) ? numberQuads : (rank + 1) * (numberQuads / size);

    double xL, xR, fL, fR;
    int first = 1;

    for (long long i = local_start; i < local_end; i++) {
        xL = a + i * width;

        if (first) {
            fL = func(xL);
            first = 0;
        } else {
            fL = fR;
        }
        xR = xL + width;
        fR = func(xR);

        local_integral += (fL + fR) * halfW;
    }

    // Reduce local integrals to global integral on rank 0
    MPI_Reduce(&local_integral, &global_integral, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        wallEnd = MPI_Wtime();
        printf("WALL CLOCK Time: %f seconds\n", wallEnd - wallStart); // added to track time
        printf("(MPI version using %d ranks): integral = %f\n", size, global_integral);
        printf("FUNC CALL COUNT: %lld\n", get_func_call_count());
    }

    // Finalize MPI
    MPI_Finalize();
    return 0;
}