#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())
    

@app.route("/")
def default_response():
    default_event = {'event_type': 'default',
                     'name': 'doing_nothing',
                     'strength':'NA',
                     'price': 'NA'}
    log_to_kafka('events', default_event)
    return "What are you waiting for?\n"


@app.route("/purchase_a_sword/", methods=['POST','GET'])
def purchase_a_sword():
    """
    @function: This function generate a Purchase a Sword event from a user mobile device request or Apache Bench
    @param: User Request (via URL endpoint) 
    @return: Returns string of User Id and Event 
    """

    userid = request.args.get('userid', default='001', type=str)
    n = request.args.get('n',default=1,type=int)
    userid = userid.replace("'", '')

    purchase_sword_event = {'userid':userid,
                            'event_type': 'purchase_sword',
                            'name': 'excalibur',
                            'strength': 1000,
                            'n_purchased': n,
                            'price': 2000}
    log_to_kafka('events', purchase_sword_event)
    return "USER " + userid + ": "+ str(n)+" "+ " Sword(s) Purchased!\n"


@app.route("/join_guild/", methods=['POST','GET'])
def join_guild():
    """
    @function: This function generate a Join Guild event from a user mobile device request or Apache Bench
    @param: User Request (via URL endpoint) 
    @return: Returns string of User Id and Event 
    """
    userid = request.args.get('userid', default='001', type=str)
    guild_name = request.args.get('guild_name', default="'The_Avengers'", type=str)
    n = request.args.get('n',default=1,type=int)

    if guild_name == "'Game_of_Thrones'":
        price = 2000
        strength = 1500
    elif guild_name == "'Castle_of_Rock'":
        price = 1000
        strength = 1200
    else: 
        price = 3000
        strength = 5000
        
    userid = userid.replace("'", '')
    guild_name = guild_name.replace("'", '')
    join_guild_event = {'userid': userid,
                        'event_type': 'join_guild',
                        'name': guild_name,
                        'strength': strength,
                        'n_purchased': 1,
                        'price': price}
    log_to_kafka('events', join_guild_event)
    return "USER: " + userid +" Joined" +" "+ guild_name +" "+ "Guild!\n"


@app.route("/purchase_a_knife/", methods=['POST','GET'])
def purchase_a_knife():
    userid = request.args.get('userid', default='001', type=str)
    n = request.args.get('n',default=1,type=int)
    userid = userid.replace("'", '')

    purchase_knife_event = {'userid': userid,
                            'event_type': 'purchase_knife',
                            'name': 'kukri',
                            'strength': 500,
                            'n_purchased': n,
                            'price': 1000}
    log_to_kafka('events', purchase_knife_event)
    return "USER " + userid + ": "+ str(n)+" "+ " Knife(s) Purchased!\n"


@app.route("/purchase_a_shield/", methods=['POST','GET'])
def purchase_a_shield():
    
    userid = request.args.get('userid', default='001', type=str)
    n = request.args.get('n',default=1,type=int)
    userid = userid.replace("'", '')

    purchase_shield_event = {'userid': userid,
                             'event_type': 'purchase_shield',
                             'name': 'parma',
                             'strength': 800,
                             'n_purchased': n,
                             'price': 1500}
    log_to_kafka('events', purchase_shield_event)
    return "USER " + userid + ": "+ str(n)+" "+ " Shield(s) Purchased!\n"

@app.route("/fight_event/", methods=['POST','GET'])
def fight_event():
    """
    @function: This function generate a Join Guild event from a user mobile device request or Apache Bench
    @param: User Request (via URL endpoint) 
    @return: Returns string of User Id and Event 
    """
    userid = request.args.get('userid', default='001', type=str)
    win_status = request.args.get('win_status', default="'won'", type=str)
    n = request.args.get('n',default=1,type=int)

    if win_status == "'lost'":
        score = -10
    else: 
        score = 100

    userid = userid.replace("'", '')
    win_status = win_status.replace("'", '')
        
    fight_event = {'userid': userid,
                   'event_type': 'fight_event',
                   'win_status': win_status,
                   'score': score}
    log_to_kafka('events', fight_event)
    return "Fight event: USER " + userid + " "+ win_status +" "+ ". Score: " + str(score) + "\n"
