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
 * This shows how to use the hdf5 virtual file drivers.
 * The example codes here do not check return values for the
 * sake of simplicity.  As in all proper programs, return codes
 * should be checked.
 */

#include "hdf5.h"
#include "stdlib.h"

/* global variables */
int cleanup_g	=	-1;	/* whether to clean.  Init to not set. */

/* prototypes */
void cleanup(const char *);
void split_file(void);


/*
 * Cleanup a file unless $HDF5_NOCLEANUP is set.
 */
void
cleanup(const char *filename)
{
    if (cleanup_g == -1)
	cleanup_g = getenv("HDF5_NOCLEANUP") ? 0 : 1;
    if (cleanup_g)
	remove(filename);
}


/*
 * This shows how to use the split file driver.
 */
void
split_file(void)
{
    hid_t fapl, fid;

    /* Example 1: Both metadata and rawdata files are in the same  */
    /*    directory.   Use Station1-m.h5 and Station1-r.h5 as      */
    /*    the metadata and rawdata files.                          */
    fapl = H5Pcreate(H5P_FILE_ACCESS);
    //H5Pset_fapl_stdio(fapl);
    H5Pset_fapl_memfs(fapl);
    fid=H5Fcreate("Station1.h5",H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
    printf("File created\n");
    /* using the file ... */
    H5Fclose(fid);
    H5Pclose(fapl);
    /* Remove files created */
    //cleanup("Station1.h5");

}


/* Main Body */
int
main (void)
{

    split_file();

    return(0);
}
