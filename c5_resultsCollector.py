from kafka import KafkaConsumer
import cProfile
import cStringIO
import pstats

#Profile block 1
profile = False

if profile:
    pr = cProfile.Profile()
    pr.enable()
#End of Profile block 1

try:
    consumer = KafkaConsumer('results'
                             ,bootstrap_servers=['localhost:9092']
                             , group_id='apoorva-thesis')
except Exception as e:
    print("Exception in Kafka consumer in scoreCollector: " + e.message)
finally:
    print "Listening to all results!"

for msg in consumer:
    if "__init__" not in msg:
        print "Result for ",msg.key," is: ",msg.value


#Profile block 2
if profile:
        pr.disable()
        s = cStringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print s.getvalue()
#End of profile block 2