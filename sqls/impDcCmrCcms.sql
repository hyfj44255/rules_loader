import from "${workingDir}/../csvs/dcCmrCcms.csv" of del METHOD P ( 1,2,3,4 ) commitcount 10000 messages "${workingDir}/../logs/impDcCmrCcms_db2.log" INSERT into CMRDC.${dcCmrCcms} (DC_CCMS ,CMR_CCMS ,Mpp_num,country);