import sqlalchemy as sa
import datetime

from automator_settings import POSTGRES_PASSWORD, POSTGRES_USERNAME


# Default connection to the address of the head node - should be 192.168.1.5
def connect(user, password, db, host='192.168.1.5', port=5432):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is the connection object
    con = sa.create_engine(url, client_encoding='utf8')

    # Bind the connection to MetaData()
    meta = sa.MetaData(bind=con, reflect=True)

    return con, meta


def update_db(date, year, genus, lab, source, amendment_flag, amended_id):
    con, meta = connect(user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, db='autoroga')

    ROGA_ID_SEQ = sa.Sequence('roga_id_seq')

    try:  # Create table if it doesn't already exist
        autoroga_project_table = sa.Table('autoroga_project_table', meta,
                                          sa.Column('id', sa.INTEGER, ROGA_ID_SEQ,
                                                    primary_key=True, server_default=ROGA_ID_SEQ.next_value()),
                                          sa.Column('roga_id', sa.String(64)),
                                          sa.Column('genus', sa.String(64)),
                                          sa.Column('lab', sa.String(16)),
                                          sa.Column('source', sa.String(64)),
                                          sa.Column('amendment_flag', sa.String(16)),
                                          sa.Column('amended_id', sa.String(64)),
                                          sa.Column('date', sa.Date),
                                          sa.Column('time', sa.DateTime, default=datetime.datetime.utcnow),
                                          sa.Column('deletion_date', sa.Date),
                                          sa.Column('deletion_reason', sa.String(256))
                                          )
        meta.create_all()
        print('Successfully created autoroga_project_table')

    except:  # Retrieve table if it already exists
        autoroga_project_table = sa.Table('autoroga_project_table', meta, autoload=True, autoload_with=sa.engine)
        print('Successfully retrieved autoroga_project_table')

    # Grab what the next key value will be
    select_next_value = sa.select([autoroga_project_table.c.id])
    keys = con.execute(select_next_value)

    try:
        next_val = max(keys)[0] + 1
    except:
        next_val = 1

    # Create ROGA ID
    roga_id = year + '-ROGA-' + '{:04d}'.format(next_val)

    # Insert new row into autoroga_project_table table
    ins = autoroga_project_table.insert().values(roga_id=roga_id, genus=genus, date=date, lab=lab, source=source,
                                                 amendment_flag=amendment_flag, amended_id=amended_id,
                                                 time=datetime.datetime.utcnow())
    con.execute(ins)
    return roga_id
