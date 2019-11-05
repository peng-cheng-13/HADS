/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 *   This example reads hyperslab from the SDS.h5 file
 *   created by h5_write.c program into two-dimensional
 *   plane of the three-dimensional array.
 *   Information about dataset in the SDS.h5 file is obtained.
 */

#include "hdf5.h"

#define H5FILE_NAME        "/Station1.h5"
#define DATASETNAME "IntArray"
#define NX_SUB  3           /* hyperslab dimensions */
#define NY_SUB  4
#define NX 5           /* output buffer dimensions */
#define NY 6
#define NZ  3
#define RANK         1
#define RANK_OUT     1

int main (void) {
    hid_t       file, dataset;         /* handles */
    hid_t       datatype, dataspace;
    hid_t       memspace;
    hid_t fapl;
    H5T_class_t t_class;                 /* data type class */
    H5T_order_t order;                 /* data order */
    size_t      size;                  /*
				        * size of the data element
				        * stored in file
				        */
    hsize_t     dimsm[3];              /* memory space dimensions */
    hsize_t     dims_out[2];           /* dataset dimensions */
    herr_t      status;

    int         data_out[NX][NY]; /* output buffer */

    int          i, j, k;

    for (j = 0; j < NX; j++) {
	for (i = 0; i < NY; i++) {
		data_out[j][i] = 0;
        }
    }

    /*
     * Open the file and the dataset.
     */
    fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_memfs(fapl);

    file = H5Fopen(H5FILE_NAME, H5F_ACC_RDONLY, fapl);
    dataset = H5Dopen2(file, DATASETNAME, H5P_DEFAULT);

    /*
     * Read data from hyperslab in the file into the hyperslab in
     * memory and display.
     */
    status = H5Dread(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
		     H5P_DEFAULT, data_out);
    for (j = 0; j < NX; j++) {
	for (i = 0; i < NY; i++) printf("%d ", data_out[j][i]);
	printf("\n");
    }
    /*
     * 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0
     * 0 0 0 0 0 0 0
     * 3 4 5 6 0 0 0
     * 4 5 6 7 0 0 0
     * 5 6 7 8 0 0 0
     * 0 0 0 0 0 0 0
     */

    /*
     * Close/release resources.
     */
    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}
