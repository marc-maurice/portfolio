# Hey! You're here. You made it. Lets' get some session ids created then look at some other cool things, too!

# Pandas will do all of the fun work and uuid will help us generate UUIDs
import pandas as pd
import uuid

# Let's get out data!
data = pd.read_csv('/Users/marc/coding/session_challenge/test1.csv')

# nulls are good, but we wanna add some fill NA's in the Delta time field. Can you guess why? Did you say because a null delta time indicates the start of a new session. Wow, you're good!
data['DELTA_TIME'] = data['DELTA_TIME'].fillna(999)

# Now we have fun. Let's make a little data frame that gives us all of the inital events of a session.
df2 = data[data['DELTA_TIME']>=30]
print(df2)

# Now let's give them some session ids!
df2['session_id'] = df2.apply(lambda _: uuid.uuid4(), axis=1)
print(df2)

# Let's reunite all of our data!
df = pd.merge(data, df2[['EVENT_ID','session_id']], how='left', on='EVENT_ID')
print(df)

# And put our data back in order, but leave some session IDs null
df.sort_values(by=['ANONYMOUS_ID','EVENT_TIME'], inplace=True)
print(df)

# Answer 1 - This is tht moment we have been waiting for. We will forward fill the session ids in this data frame to get us session ids on each event!
df['session_id'].fillna(method = 'ffill', inplace = True)
print(df)

# I am curious to see how many event we have in each session id.
print(df['session_id'].value_counts())
# Why did '59d33c40-8944-4a47-98db-08be90fd5865' only visit one page?

# Answer 2 - identify the number of sessions that contain a specific URL.
print(df.groupby(['URL']).nunique()['session_id'])

# Bonus thoughts
# I would recommend batch processing on some cadence depending on volume and business demands
# create a table which holds the last event time for each anon_ids
# batch processing on incoming data could use the table to check for anon_ids
# with web traffic, we can often have data which lags behind, so we would need logic to handle this
# with web traffic, anon_ids are sometimes reattributed at a later date, so we would need logic to handle this
