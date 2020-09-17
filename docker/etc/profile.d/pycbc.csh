# Singularity already sets up the login environment
if ( x"${SINGULARITY_CONTAINER}" == "x" ) then 

    # Use the lal-data bundled in the image if not set
    if ( "x${LAL_DATA_PATH}" == "x" ) then
        setenv LAL_DATA_PATH /opt/pycbc/pycbc-software/share/lal-data
    endif

endif
