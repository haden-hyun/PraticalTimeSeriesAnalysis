import pandas as pd
import io
import requests

# dataload
YearJoined = pd.read_csv("https://raw.githubusercontent.com/PracticalTimeSeriesAnalysis/BookRepo/master/Ch02/data/year_joined.csv")
emails = pd.read_csv("https://raw.githubusercontent.com/PracticalTimeSeriesAnalysis/BookRepo/master/Ch02/data/emails.csv")
donations = pd.read_csv("https://raw.githubusercontent.com/PracticalTimeSeriesAnalysis/BookRepo/master/Ch02/data/donations.csv")

YearJoined.groupby('user').count().groupby('userStats').count()

emails[emails.emailsOpened < 1]

emails[emails.user == 998]


import datetime
date_time_str_max = max(emails[emails.user == 998].week)
date_time_str_min = min(emails[emails.user == 998].week)

date_time_obj_max = datetime.datetime.strptime(date_time_str_max, '%Y-%m-%d %H:%M:%S')
date_time_obj_min = datetime.datetime.strptime(date_time_str_min, '%Y-%m-%d %H:%M:%S')

(date_time_obj_max - date_time_obj_min).days/7


complete_idx = pd.MultiIndex.from_product([set(emails.week), set(emails.user)])

'''완전한 시계열 데이터 생성''' 
all_email = emails\
    .set_index(['week', 'user'])\
    .reindex(complete_idx, fill_value = 0)\
    .reset_index()
all_email.columns = ['week','user','emailsOpened']
all_email['week'] = pd.to_datetime(all_email['week'])
all_email[all_email.user == 998].sort_values('week')


'''그룹별 최소/최대 날짜 계산'''
cutoff_dates = emails.groupby('user').week.agg(['min', 'max']).reset_index()
cutoff_dates = cutoff_dates.reset_index()

import warnings
warnings.filterwarnings('ignore')

for _, row in cutoff_dates.iterrows():
    user        = row['user']
    start_date  = row['min']
    end_date    = row['max']
    # 해당 user 최초 열람 이전 주 삭제
    all_email.drop(all_email[all_email.user == user][all_email.week < start_date].index, inplace=True)
    # 해당 user 최종 열람 이후 주 삭제
    all_email.drop(all_email[all_email.user == user][all_email.week > end_date].index, inplace=True)


'''기부 자료 주단위 변경(다운샘플링)'''
donations.timestamp = pd.to_datetime(donations.timestamp)
donations.set_index('timestamp', inplace=True) # 리샘플링에 필요한 작업
agg_don = donations.groupby('user').apply(lambda df: df.amount.resample("W-MON").sum().dropna())


'''주간 이메일 및 기부자료 결합'''
merged_df = pd.DataFrame()

for user, user_email in all_email.groupby('user'):
    user_donations = agg_don[agg_don.index.get_level_values('user') == user]
    
    user_donations = user_donations.droplevel(0)
    # user_donations.set_index('timestamp', inplace = True)  
    
    user_email = all_email[all_email.user == user]
    user_email.sort_values('week', inplace=True)
    user_email.set_index('week', inplace=True)

    df = pd.merge(user_email, user_donations, how='left', left_index=True, right_index=True)
    df.fillna(0, inplace=True)

    merged_df = pd.concat([merged_df, df.reset_index()[['user', 'week', 'emailsOpened', 'amount']]])

merged_df[merged_df.user == 998]

df = merged_df[merged_df.user == 998]
df['target'] = df.amount.shift(1)
df = df.fillna(0)
df


# page 57
air = pd.read_csv("https://raw.githubusercontent.com/PracticalTimeSeriesAnalysis/BookRepo/master/Ch02/data/AirPassengers.csv", names=['Date', 'Passengers'])

air['Smooth.5'] = air.ewm(alpha = .5).Passengers.mean()
air['Smooth.9'] = air.ewm(alpha = .9).Passengers.mean()

# 64 page
import datetime
datetime.datetime.utcnow()
datetime.datetime.now()
datetime.datetime.now(datetime.timezone.utc)

import pytz

western = pytz.timezone('US/Pacific')
western.zone

loc_dt = western.localize(datetime.datetime(2018, 5, 15, 12, 34, 0))
loc_dt

london_tz = pytz.timezone('Europe/London')
london_dt = loc_dt.astimezone(london_tz)
london_dt

f = '%Y-%m-%d %H:%M:%S %Z%z'
datetime.datetime(2018, 5, 12, 12, 15, 0, tzinfo = london_tz).strftime(f)

event1 = datetime.datetime(2018, 5, 12, 12, 15, 0, tzinfo = london_tz)
event2 = datetime.datetime(2018, 5, 13, 9, 15, 0, tzinfo = western)
event2 - event1

event1 = london_tz.localize(datetime.datetime(2018, 5, 12, 12, 15, 0))
event2 = western.localize(datetime.datetime(2018, 5, 13, 9, 15, 0))
event2 - event1

event1 = london_tz.localize((datetime.datetime(2018, 5, 12, 12, 15, 0))).astimezone(datetime.timezone.utc)
event2 = western.localize(datetime.datetime(2018, 5, 13, 9, 15, 0)).astimezone(datetime.timezone.utc)
event2 - event1

pytz.common_timezones

pytz.country_timezones('fr')

ambig_time = western.localize(datetime.datetime(2002, 10, 27, 1, 30, 00)).astimezone(datetime.timezone.utc)
ambig_time_earlier = ambig_time - datetime.timedelta(hours=1)
ambig_time_later = ambig_time + datetime.timedelta(hours=1)

ambig_time_earlier.astimezone(western)

ambig_time.astimezone(western)

ambig_time_later.astimezone(western)

ambig_time = western.localize(datetime.datetime(2002, 10, 27, 1, 30, 00), is_dst = True).astimezone(datetime.timezone.utc)
ambig_time_earlier = ambig_time - datetime.timedelta(hours=1)
ambig_time_later = ambig_time + datetime.timedelta(hours=1)

ambig_time_earlier.astimezone(western)

ambig_time.astimezone(western)

ambig_time_later.astimezone(western)