Tools for post-processing CCAM data after DRS-ing raw CCAM data in axiom

aus20i_concat_day_mon
  - Used to concatenate daily and monthly data as per CORDEX-CMIP6 AUS-20i standards
 
aus20i_fix_reconcatenate_individual_files
  - Used to reconcatenate individual daily/monthly files if a single year needs to be reprocessed in axiom
  
aus20i_check_all
  - Used to check all metadata for AUS-20i to ensure it meets CORDEX-CMIP6 specifications. Can also be used to fix metadata with "--fix" flag in check_all.pbs

To do:
  - Modify aus20i_concat_day_mon scripts to use xarray instead of CDO. CDO inserts metadata and doesn't compress as well
