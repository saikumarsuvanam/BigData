wget https://www.gutenberg.org/files/98/98-0.txt

sed -e 's/[^[:alpha:]]/ /g' 98-0.txt | tr '\n' " " | tr -s " " | tr " " '\n'| tr 'A-Z' 'a-z' | sort | uniq -c | sort -nr | nl | awk '{ print $3"            "$2}' > sxm155431Part1.txt


 

