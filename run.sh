#!/bin/bash

# First line is only needed to compile. Please take it out if you are scoring based on run time!
csc /t:exe /out:Program.exe ./src/Program.cs
Program.exe ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt
