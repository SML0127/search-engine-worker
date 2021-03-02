

file=$1

#scp -P 20220 $file 141.223.197.33:~/pse-driver/$file
#scp -P 20220 $file 141.223.197.32:~/PSE-engine/$file
#scp -P 20220 $file 141.223.197.34:~/PSE-engine/$file
#scp -P 20220 141.223.197.35:~/pse-driver/$file ./$file
echo 'scp to 141.223.197.33'
scp -P 20220 $file 141.223.197.33:~/PSE-engine/$file
echo 'scp to 141.223.197.34'
scp -P 20220 $file 141.223.197.34:~/PSE-engine/$file
echo 'scp to 141.223.197.37'
scp -P 20220 $file 141.223.197.37:~/PSE-engine/$file
echo 'scp to 141.223.197.38'
scp -P 20220 $file 141.223.197.38:~/PSE-engine/$file
