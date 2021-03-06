#! /usr/bin/bash

# usage: ./data_generator.sh -u 15 -e 5 -n 20 -b

helpFunction()
{
   echo ""
   echo "Welcome CARLOS 2020 to mids 205 Project 3 synthetic data generator"
   echo "Usage: $0 -u NOOFUSERS -e ENDPOINTS -n GENERATEREQS"
   echo -e "\t-u Number of users"
   echo -e "\t-e Number of endpoints"
   echo -e "\t-n Number of total requests"
   echo -e "\t-b (Optional) use Apache Bench to send the requests to flask, uses no args just the flag"
   exit 1 # Exit script after printing help
}

while getopts "u:e:n:b" opt
do
   case "$opt" in
      u ) NOOFUSERS="$OPTARG" ;;
      e ) ENDPOINTS="$OPTARG" ;;
      n ) GENERATEREQS="$OPTARG" ;;
      b ) ABFLAG="SET" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$NOOFUSERS" ] || [ -z "$ENDPOINTS" ] || [ -z "$GENERATEREQS" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi


## ** Set limits to per user items
REQS=0
MAXNOOFSWORDS=10 # Max swords purchased at a time per user
MAXNOOFSHIELDS=10 # max shields purchased at a time per user
MAXNOOFKNIFES=10 # max potions purchased at a time per user
MAXGUILDS=3 # max number of guilds joined at a time per user
CONCURRENTUSERS=1 #max users accessing the flask API (* Cannot use concurrency level greater than total number of requests [CONCURRENTUSERS < GENERATEREQS] )
RANDVAR=2

## Event Types will be randomly assigned to a number between 1 and 9 based on endpoints specified.
# 1) Purchase a Sword
# 2) Purchase a Shield
# 3) Purchase a Knife
# 4) Join a Guild - this randomly generate 1 of three guild names Game of Thrones, Castle of Rock, and Knights of the Round table
# 5) fight_event

## ** Check if apache bench optional param is passed 
if [ "$ABFLAG" ]
then
    echo "Apache Bench flag is $ABFLAG";
    # docker exec -it project-3-elizkhan_mids_1 ab -n 2 -H "Host: liz.comcast.com" 'http://localhost:5000/purchase_a_potion/?userid=002&n=10' ## works
    # docker-compose exec mids ab -n 2 -H "Host: liz.comcast.com" http://localhost:5000/ #does not work in windows
    until [ $REQS -gt $GENERATEREQS ]; do
        ID=$(( ( RANDOM % $NOOFUSERS )  + 1 ))
        EP=$(( ( RANDOM % $ENDPOINTS )  + 1 ))
        NOOFSWORDS=$(( ( RANDOM % $MAXNOOFSWORDS )  + 1 ))
        NOOFSHIELDS=$(( ( RANDOM % $MAXNOOFSHIELDS )  + 1 ))
        NOOFKNIFES=$(( ( RANDOM % $MAXNOOFKNIFES )  + 1 ))
        case $EP in
            1)
            docker-compose exec mids ab -n 5 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/purchase_a_sword/?userid=%27user-00$ID%27&n=$NOOFSWORDS"
            ;;
        esac
        case $EP in
            2)
            docker-compose exec mids ab -n 4 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/purchase_a_shield/?userid=%27user-00$ID%27&n=$NOOFSHIELDS"
            ;;
        esac
        case $EP in
            3)
            docker-compose exec mids ab -n 4 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/purchase_a_knife/?userid=%27user-00$ID%27&n=$NOOFKNIFES"
            ;;
        esac    
        case $EP in
            4)
              GUILDID=$(( ( RANDOM % $MAXGUILDS )  + 1 ))
              case $GUILDID in
                1)
                docker-compose exec mids ab -n 2 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27"
                ;;
                2)
                docker-compose exec mids ab -n 2 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27&guild_name=%27Game_of_Thrones%27"
                ;;
                3)
                docker-compose exec mids ab -n 2 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27&guild_name=%27Justice_League%27"
                ;;
              esac
        esac
        case $EP in
                5)
                WINFLAG=$(( ( RANDOM % $RANDVAR ) + 1))
                case $WINFLAG in
                   1)
                   docker-compose exec mids ab -n 2 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/fight_event/?userid=%27user-00$ID%27&win_status=%27lost%27"
                   ;;
                   2)
                   docker-compose exec mids ab -n 2 -H "Host: user-00$ID.comcast.com" "http://localhost:5000/fight_event/?userid=%27user-00$ID%27&win_status=%27won%27"               
                   ;;
                 esac
        esac
        let REQS=REQS+1
    done
else
    until [ $REQS -gt $GENERATEREQS ]; do
        ID=$(( ( RANDOM % $NOOFUSERS )  + 1 ))
        EP=$(( ( RANDOM % $ENDPOINTS )  + 1 ))
        NOOFSWORDS=$(( ( RANDOM % $MAXNOOFSWORDS )  + 1 ))
        NOOFSHIELDS=$(( ( RANDOM % $MAXNOOFSHIELDS )  + 1 ))
        NOOFKNIFES=$(( ( RANDOM % $MAXNOOFKNIFES )  + 1 ))
        case $EP in
            1)
            docker-compose exec mids curl "http://localhost:5000/purchase_a_sword/?userid=%27user-00$ID%27&n="$NOOFSWORDS
            ;;
        esac
        case $EP in
            2)
            docker-compose exec mids curl "http://localhost:5000/purchase_a_shield/?userid=%27user-00$ID%27&n="$NOOFSHIELDS
            ;;
        esac
        case $EP in
            3)
            docker-compose exec mids curl "http://localhost:5000/purchase_a_knife/?userid=%27user-00$ID%27&n="$NOOFKNIFES
            ;;
        esac
        case $EP in
            4)
            GUILDID=$(( ( RANDOM % $MAXGUILDS )  + 1 ))
              case $GUILDID in
                1)
                docker-compose exec mids curl "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27"
                ;;
                2)
                docker-compose exec mids curl "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27&guild_name=%27Game_of_Thrones%27"
                ;;
                3)
                docker-compose exec mids curl "Host: user-00$ID.comcast.com" "http://localhost:5000/join_guild/?userid=%27user-00$ID%27&guild_name=%27Justice_League%27"
                ;;
              esac
        esac
        case $EP in
            5)
             WINFLAG=$(( ( RANDOM % $RANDVAR ) + 1))
             case $WINFLAG in
               1)
               docker-compose exec mids curl "Host: user-00$ID.comcast.com" "http://localhost:5000/fight_event/?userid=%27user-00$ID%27&win_status=%27lost%27"
               ;;
               2)
               docker-compose exec mids curl "Host: user-00$ID.comcast.com" "http://localhost:5000/fight_event/?userid=%27user-00$ID%27&win_status=%27won%27"
               ;;
             esac
        esac
        let REQS=REQS+1
    done
fi

#$RANDOM%2
