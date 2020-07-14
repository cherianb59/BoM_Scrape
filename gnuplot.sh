date=$(date  +%Y-%m-%d)

echo 'set terminal png size 3000,800' > gnuplot.txt
echo 'set xdata time' >>gnuplot.txt
echo 'set timefmt "%Y%m%d%H%M%S"' >>gnuplot.txt
echo "set output \"./$date.png\"" >>gnuplot.txt
echo 'set yrange [:]' >>gnuplot.txt
echo 'set xrange [:]' >>gnuplot.txt
echo 'set grid' >>gnuplot.txt
echo 'set xlabel "\\nTime"' >>gnuplot.txt
echo 'set key left box' >>gnuplot.txt
echo " plot \"../canberra/$date-canberra.csv\" using 1:2 index 0 title \" Canberra Air Temp\" with lines , \\
\"../canberra/$date-canberra.csv\" using 1:3  title \"Canberra Apparent Temp\" with lines, \\
\"../centralcoast/$date-centralcoast.csv\" using 1:2  title \"Gosford Air Temp\" with lines,\\
\"../centralcoast/$date-centralcoast.csv\" using 1:3  title \"Gosford Apparent Temp\" with lines" >> gnuplot.txt
cat gnuplot.txt|gnuplot
