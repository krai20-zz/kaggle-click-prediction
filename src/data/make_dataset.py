import pandas as pd

def read_data(filepath):
    df = pd.read_csv(filepath, compression='zip')
    return df


def rename_cols(pc, doc_meta, doc_cats, doc_topics):
    pc = pc.rename(columns={
                    'document_id':'ad_document_id',
                    'campaign_id':'ad_campaign_id',
                    'advertiser_id':'ad_advetiser_id'
                    })
    doc_meta = doc_meta.rename(columns={
                    'document_id':'ad_document_id',
                    'source_id':'ad_document_source',
                    'publisher':'ad_document_publisher'
                    })
    doc_cats = doc_cats.rename(columns={
                    'document_id':'ad_document_id',
                    'topic_id':'ad_document_topic_id',
                    'confidence_level':'ad_doc_topic_confidence_level'
                    })
    doc_topics = doc_topics.rename(columns={
                    'document_id':'ad_document_id',
                    'category_id':'ad_document_category_id',
                    'confidence_level':'ad_doc_category_confidence_level'
                    })
    return pc, doc_meta, doc_cats, doc_topics


def merge_data(train, events, pc, doc_meta, doc_cats, doc_topics):
    """
    Merges training data, events, and data about ads and ads landing pages
    """
    pc, doc_meta, doc_cats, doc_topics = rename_cols(pc, doc_meta, doc_cats, doc_topics)
    df1 = train.merge(events, on='display_id', how='left')
    df2 = df1.merge(pc, on='ad_id', how='left')
    df3 = df2.merge(doc_meta, on='ad_document_id', how='left')
    df4 = df3.merge(doc_cats, on='ad_document_id', how='left')
    main_df = df4.merge(doc_topics, on='ad_document_id', how='left')

    return main_df


def run(train_path, events_path, pc_path, doc_meta_path, doc_cats_path, doc_topics_path):
    print "reading train"
    train = read_data(train_path)
    print "reading events"
    events = read_data(events_path)
    print "reading pc"
    pc = read_data(pc_path)
    print "reading meta"
    doc_meta = read_data(doc_meta_path)
    print "reading cat"
    doc_cats = read_data(doc_cats_path)
    print "reading topic"
    doc_topics = read_data(doc_topics_path)
    print "merging files"
    main_df = merge_data(train, events, pc, doc_meta, doc_cats, doc_topics)


if __name__ == "__main__":
    train_path = '../../data/raw/clicks_train.csv.zip'
    events_path = '../../data/raw/events.csv.zip'
    pc_path = '../../data/raw/promoted_content.csv.zip'
    doc_meta_path = '../../data/raw/documents_meta.csv.zip'
    doc_cats_path = '../../data/raw/documents_categories.csv.zip'
    doc_topics_path = '../../data/raw/documents_topics.csv.zip'
    run(train_path, events_path, pc_path, doc_meta_path, doc_cats_path, doc_topics_path)
