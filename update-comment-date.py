from collections import defaultdict
from datetime import datetime, timedelta, timezone
from os import getenv
from rule_scout import NotionApi, NOTION_RULE_DATABASE, RegulationsGovApi, notion_rich_text_url_list


def parse_rich_text_list(notion_object: dict) -> list[str]:
    text = notion.cell_as_text(notion_object)
    if text:
        return [item.strip() for item in text.split(',')]
    else:
        return []


with NotionApi(getenv('NOTION_API_KEY')) as notion:
    with RegulationsGovApi(getenv(key='REGULATIONS_GOV_API_KEY')) as regulations_gov:
        pages = defaultdict(list)
        # rule_rows = notion.query_db(
        #     NOTION_RULE_DATABASE,
        #     {
        #         'property': 'Comment End Date',
        #         'date': {
        #             'after': (datetime.now(tz=timezone.utc) - timedelta(days=-40)).isoformat()
        #         }
        #     }
        # )
        a_month_ago_iso = (datetime.now(tz=timezone.utc) - timedelta(days=31)).isoformat()
        rule_rows = notion.query_db(
            NOTION_RULE_DATABASE,
            {
                'or': [
                    {
                        'property': 'Comment End Date',
                        'date': {
                            'on_or_after': a_month_ago_iso
                        }
                    },
                    {
                        'and': [
                            {
                                'property': 'Comment End Date',
                                'date': {
                                    'is_empty': True
                                }
                            },
                            {
                                'property': 'FR Publication Date',
                                'date': {
                                    'on_or_after': a_month_ago_iso
                                }
                            }
                        ]
                    }
                ]
            },
            sort={'FR Publication Date': 'ascending'}
        )

        for page in rule_rows:
            updates = {}

            fr_number = notion.cell_as_text(page['properties']['FR Document Number'])
            old_comment_deadline_iso = page['properties'].get('Comment End Date')
            old_comment_deadline = notion.cell_as_datetime(old_comment_deadline_iso) if old_comment_deadline_iso else None

            old_docket_docs = parse_rich_text_list(page['properties']['Docket Documents'])
            old_dockets = parse_rich_text_list(page['properties']['Dockets'])

            print(f'{fr_number}: {old_comment_deadline} - {old_docket_docs}')

            doc_infos = regulations_gov.find_documents_by_register_id(fr_number)
            found_docs = []
            found_dockets = []
            latest_comment_date = old_comment_deadline
            for found in doc_infos:
                found_id = found['id']
                found_docs.append(found_id)
                found_docket = found['attributes']['docketId']
                if found_docket:
                    found_dockets.append(found_docket)
                if found['attributes']['commentEndDate']:
                    found_comment_date = datetime.fromisoformat(found['attributes']['commentEndDate'])
                    if not found_comment_date.tzinfo:
                        found_comment_date = found_comment_date.astimezone(timezone.utc)
                    # Notion dates only have minute-level precision.
                    found_comment_date = found_comment_date.replace(second=0, microsecond=0)
                    if (not latest_comment_date) or found_comment_date > latest_comment_date:
                        latest_comment_date = found_comment_date

            for missing in set(old_docket_docs).difference(found_docs):
                print(f'  Lost doc! {missing}')
            for missing in set(old_dockets).difference(found_dockets):
                print(f'  Lost docket: {missing}')
            for added in set(found_docs).difference(old_docket_docs):
                print(f'  New doc: {added}')
            for added in set(found_dockets).difference(old_dockets):
                print(f'  New docket: {added}')
            if set(old_docket_docs) != set(found_docs):
                updates['Docket Documents'] = {
                    'type': 'rich_text',
                    'rich_text': notion_rich_text_url_list(
                        (d, f'https://www.regulations.gov/document/{d}')
                        for d in sorted(found_docs)
                    )
                }
            if set(old_dockets) != set(found_dockets):
                updates['Dockets'] = {
                    'type': 'rich_text',
                    'rich_text': notion_rich_text_url_list(
                        (d, f'https://www.regulations.gov/docket/{d}')
                        for d in sorted(found_dockets)
                    )
                }

            if old_comment_deadline != latest_comment_date:
                print(f'  New comment deadline: {latest_comment_date}')
                updates['Comment End Date'] = {
                    'type': 'date',
                    'date': {
                        'start': latest_comment_date.isoformat()
                    } if latest_comment_date else None
                }

            if updates:
                print(f'  Updates: {updates}')
                # notion.update_page(page['id'], updates)
