import sqlalchemy as sa

from automators_dev.postgres_settings import POSTGRES_PASSWORD, POSTGRES_USERNAME


def connect(user, password, db, host='localhost', port=5432):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sa.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sa.MetaData(bind=con, reflect=True)

    return con, meta


def update_db(date, year, genus, lab, source):
    con, meta = connect(user=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, db='autoroga')

    ROGA_ID_SEQ = sa.Sequence('roga_id_seq')

    try:
        autoroga_project_table = sa.Table('autoroga_project_table', meta,
                                          sa.Column('roga_id', sa.INTEGER, ROGA_ID_SEQ, primary_key=True, server_default=ROGA_ID_SEQ.next_value()),
                                          sa.Column('genus', sa.String(64)),
                                          sa.Column('lab', sa.String(16)),
                                          sa.Column('source', sa.String(64)),
                                          sa.Column('date', sa.Date))
        meta.create_all()
        print('Successfully created autoroga_project_table')
    except:
        autoroga_project_table = sa.Table('autoroga_project_table', meta, autoload=True, autoload_with=sa.engine)
        print('Successfully retrieved autoroga_project_table')


    # Grab what the next key value will be
    select_next_value = sa.select([autoroga_project_table.c.roga_id])
    keys = con.execute(select_next_value)
    next_val = max(keys)[0] + 1

    # Insert new row into table
    ins = autoroga_project_table.insert().values(genus=genus, date=date, lab=lab, source=source)
    con.execute(ins)

    # Create report ID
    report_id = year + '-ROGA-' + '{:04d}'.format(next_val)

    return report_id