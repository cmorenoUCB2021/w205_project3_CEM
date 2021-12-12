#!/usr/bin/python

import time
import argparse
import random
import subprocess

# Sample command to run the file: python3 data_generator.py --num_users 3 --num_endpoints 5 --num_requests 30

def join_justice_league(): 
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27&guild_name=%27Justice_League%27".format(
        user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command

def join_game_of_thrones():
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27&guild_name=%27Game_of_Thrones%27".format(
        user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command


def join_a_guild():
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27".format(user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command


def purchase_knives():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_knife/?userid=%27user-00{0}%27&n={1}".format(user_id, num_knives)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command


def purchase_shields():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_shield/?userid=%27user-00{0}%27&n={1}".format(user_id,
                                                                                             num_shields)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command


def purchase_swords():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_sword/?userid=%27user-00{0}%27&n={1}".format(user_id, num_swords)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command

def fight():
    global param1, param2, docker_command
    fight_event = random.uniform(0,1)
    if fight_event > 0.5:
        win_status = "won"
    else:
        win_status = "lost"
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/fight_event/?userid=%27user-00{0}%27&win_status={1}%27".format(user_id,win_status)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_users", type=str, help="Number of users")
    parser.add_argument("--num_endpoints", type=str, help="Numer of endpoints")
    parser.add_argument("--num_requests", type=str, help="Number of total requests")
    ############
    args = parser.parse_args()
    num_users = int(args.num_users)
    num_endpoints = int(args.num_endpoints)
    num_requests = int(args.num_requests)

    print("num_users: %s" % (num_users))
    print("num_endpoints: %s" % (num_endpoints))
    print("num_requests: %s" % (num_requests))

    ### Set limits to per user items
    requsts = 0
    max_num_of_swords = 10  # Max swords purchased at a time per user
    max_num_of_shields = 10  # max shields purchased at a time per user
    max_num_of_knives = 10  # max potions purchased at a time per user
    max_num_of_guilds = 3  # max number of guilds joined at a time per user
    max_concurrent_users = 1  # max

    # print("user_id: %s" % (user_id))
    # print("end_point: %s" % (end_point))
    # print("num_knives: %s" % (num_knives))
    # print("num_swords: %s" % (num_swords))
    # print("user_id: %s" % (user_id))
    # print("user_id: %s" % (user_id))



    ### Create cache to memorized joined guilds#!/usr/bin/python

import time
import argparse
import random
import subprocess

def join_justice_league(): 
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27&guild_name=%27Justice_League%27".format(
        user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command

def join_game_of_thrones():
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27&guild_name=%27Game_of_Thrones%27".format(
        user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command


def join_a_guild():
    global param1, param2, docker_command
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/join_guild/?userid=%27user-00{0}%27".format(user_id)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command


def purchase_knives():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_knife/?userid=%27user-00{0}%27&n={1}".format(user_id, num_knives)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command


def purchase_shields():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_shield/?userid=%27user-00{0}%27&n={1}".format(user_id,
                                                                                             num_shields)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command


def purchase_swords():
    global param1, docker_command
    param1 = "http://localhost:5000/purchase_a_sword/?userid=%27user-00{0}%27&n={1}".format(user_id, num_swords)
    docker_command = 'docker-compose exec mids curl "%s"' % (param1)
    return docker_command

def fight():
    global param1, param2, docker_command
    fight_event = random.uniform(0,1)
    if fight_event > 0.5:
        win_status = "won"
    else:
        win_status = "lost"
    param1 = "Host: user-00{0}.comcast.com".format(user_id)
    param2 = "http://localhost:5000/fight_event/?userid=%27user-00{0}%27&win_status={1}%27".format(user_id,win_status)
    docker_command = 'docker-compose exec mids curl "%s" "%s"' % (param1, param2)
    return docker_command


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_users", type=str, help="Number of users")
    parser.add_argument("--num_endpoints", type=str, help="Numer of endpoints")
    parser.add_argument("--num_requests", type=str, help="Number of total requests")
    ############
    args = parser.parse_args()
    num_users = int(args.num_users)
    num_endpoints = int(args.num_endpoints)
    num_requests = int(args.num_requests)

    print("num_users: %s" % (num_users))
    print("num_endpoints: %s" % (num_endpoints))
    print("num_requests: %s" % (num_requests))

    ### Set limits to per user items
    requsts = 0
    max_num_of_swords = 10  # Max swords purchased at a time per user
    max_num_of_shields = 10  # max shields purchased at a time per user
    max_num_of_knives = 10  # max potions purchased at a time per user
    max_num_of_guilds = 3  # max number of guilds joined at a time per user
    max_concurrent_users = 1  # max

    # print("user_id: %s" % (user_id))
    # print("end_point: %s" % (end_point))
    # print("num_knives: %s" % (num_knives))
    # print("num_swords: %s" % (num_swords))
    # print("user_id: %s" % (user_id))
    # print("user_id: %s" % (user_id))



    ### Create cache to memorized joined guilds
    guild_cache = dict()

    request_counter = 0
    while request_counter < num_requests:

        user_id = random.randint(1, 100000) % num_users + 1
        end_point = random.randint(1, 100000) % num_endpoints + 1
        num_knives = random.randint(1, 100000) % max_num_of_knives + 1
        num_swords = random.randint(1, 100000) % max_num_of_swords + 1
        num_shields = random.randint(1, 100000) % max_num_of_shields + 1

        docker_command = ""
        if end_point == 1:
            docker_command = purchase_swords()
        elif end_point == 2:
            docker_command = purchase_shields()
        elif end_point == 3:
            docker_command = purchase_knives()
        elif end_point == 4:
            docker_command = fight()
        else:
            num_guilds = random.randint(1, 100000) % max_num_of_guilds + 1
            if num_guilds == 1:
                docker_command = join_a_guild()
            elif num_guilds == 2:
                docker_command = join_game_of_thrones()
            elif num_guilds == 3:
                docker_command = join_justice_league()

        print(docker_command)
        if docker_command not in guild_cache:
            docker_process = subprocess.Popen(docker_command, shell=True, executable='/bin/bash', stdout=subprocess.PIPE,
                                              stderr=subprocess.STDOUT)
            guild_cache[docker_command] = docker_process

        time.sleep(1)
        request_counter += 1
    guild_cache = dict()

    request_counter = 0
    while request_counter < num_requests:

        user_id = random.randint(1, 100000) % num_users + 1
        end_point = random.randint(1, 100000) % num_endpoints + 1
        num_knives = random.randint(1, 100000) % max_num_of_knives + 1
        num_swords = random.randint(1, 100000) % max_num_of_swords + 1
        num_shields = random.randint(1, 100000) % max_num_of_shields + 1

        docker_command = ""
        if end_point == 1:
            docker_command = purchase_swords()
            subprocess.Popen(docker_command, shell=True, executable='/bin/bash',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        elif end_point == 2:
            docker_command = purchase_shields()
            subprocess.Popen(docker_command, shell=True, executable='/bin/bash',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        elif end_point == 3:
            docker_command = purchase_knives()
            subprocess.Popen(docker_command, shell=True, executable='/bin/bash',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        elif end_point == 4:
            docker_command = fight()
            subprocess.Popen(docker_command, shell=True, executable='/bin/bash',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        else:
            num_guilds = random.randint(1, 100000) % max_num_of_guilds + 1
            if num_guilds == 1:
                docker_command = join_a_guild()
            elif num_guilds == 2:
                docker_command = join_game_of_thrones()
            elif num_guilds == 3:
                docker_command = join_justice_league()
            
            if docker_command not in guild_cache:
                print(docker_command)
                docker_process = subprocess.Popen(docker_command, shell=True, executable='/bin/bash',
                                                  stdout=subprocess.PIPE,
                                                  stderr=subprocess.STDOUT)
                guild_cache[docker_command] = docker_process

        time.sleep(1)
        request_counter += 1