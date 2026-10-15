#!/bin/bash

nohup Rscript screening-dbasRheinKobPos.R &> dbasRheinKobPos.log &
nohup Rscript screening-dbasWeilMmp2023.R &> dbasWeilMmp2023.log &
nohup Rscript screening-dbasRheinKobPosParallel.R &> dbasRheinKobPosParallel.log &
nohup Rscript screening-dbasRingversuchParallel.R &> dbasRingversuchParallel.log &
nohup Rscript screening-dbasRingversuchPosOrbi.R &> dbasRingversuchPosOrbi.log &

nohup Rscript screening-ntsRheinKobPos.R &> ntsRheinKobPos.log &
nohup Rscript screening-ntsRheinKobPosParallel.R &> ntsRheinKobPosParallel.log &
