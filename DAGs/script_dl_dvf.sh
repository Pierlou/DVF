curr_year=`date +'%Y'`
five_ago=$((curr_year-5))
echo $PWD
DATADIR="/tmp/data"
rm -rf $DATADIR
mkdir -p $DATADIR

for YEAR in `seq $five_ago $curr_year`
do
  echo $YEAR && [ ! -f $DATADIR/full_$YEAR.csv.gz ] && wget -r -np -nH -N --cut-dirs 5  https://files.data.gouv.fr/geo-dvf/latest/csv/$YEAR/full.csv.gz -O $DATADIR/full_$YEAR.csv.gz
done

# for YEAR in `seq $five_ago $curr_year`
# do
#   echo $YEAR && [ ! -f $DATADIR/full_$YEAR.csv.gz ] && curl  https://files.data.gouv.fr/geo-dvf/latest/csv/$YEAR/full.csv.gz > $DATADIR/full_$YEAR.csv.gz
# done

find $DATADIR -name '*.gz' -exec gunzip -f '{}' \;

cd $DATADIR && ls
# export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES