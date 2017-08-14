# coding:utf-8
from sync_dbq import SyncDbq

# tables = [
#     {'events': ['id', 'name', 'category_id', 'price_per_user', 'billing_type']},
#     {'users': ['id' ,'account_id', 'prefecture_id', 'gender']},
#     {'schedule_entries': ['id', 'schedule_id', 'user_id', 'state', 'is_participated', 'updated_at']},
#     {'material_downloads': ['id', 'user_id', 'material_id']},
#     {'materials': ['id', 'name', 'file']},
#     {'event_classified_tags': ['id', 'event_id', 'classified_tag_id']},
#     {'article_classified_tags': ['id', 'article_id', 'classified_tag_id']},
#     {'accounts': ['id', 'email', 'suspended_email']},
#     {'classified_tags': ['id', 'category_id', 'value', 'classified_tag_id']},
#     {'schedules': ['id', 'event_id', 'state', 'capacity', 'open_at', 'close_at', 'acceptance_at', 'deadline_at']}
# ]
tables = [{'schedules': ['id', 'event_id', 'event_place_id', 'state', 'capacity', 'open_at', 'close_at', 'acceptance_at', 'deadline_at']}]

sync_dbq = SyncDbq(user = 'root', password = '', host = 'localhost', db_name = 'chai_development', bq = 'ga.json')
sync_dbq.sync_to_bq(tables)
