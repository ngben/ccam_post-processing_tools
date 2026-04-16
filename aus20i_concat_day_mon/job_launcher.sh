#!/bin/bash

qsub concat_daily_hist.sh
qsub concat_daily_ssp126.sh
qsub concat_daily_ssp370.sh
qsub concat_monthly_hist.sh
qsub concat_monthly_ssp126.sh
qsub concat_monthly_ssp370.sh
