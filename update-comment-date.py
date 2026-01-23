from datetime import datetime, timedelta, timezone
from os import getenv
from rule_scout import (
    NOTION_RULE_DATABASE,
    Docket,
    NotionApi,
    RegulationsGovApi,
    notion_rich_text,
    notion_rich_text_url_list,
)
import time


# If true, checks for changes to metadata associated with Dockets (e.g. RINs,
# keywords) on every run, instead of only if new dockets were found.
ALWAYS_UPDATE_DOCKET_DATA = False

# Basic rate limit is 1,000/hour, or 1 request every 3.6 seconds. Based on:
# https://api.data.gov/docs/developer-manual/
REGULATIONS_GOV_REQUEST_INTERVAL = 3.6


def parse_rich_text_list(notion_object: dict) -> list[str]:
    text = notion.cell_as_text(notion_object)
    if text:
        return [item.strip() for item in text.split(',')]
    else:
        return []


def parse_multiselect_set(notion_object: dict) -> set[str]:
    if 'multi_select' not in notion_object:
        raise TypeError(f'Object is not a multi_select, it is "{notion_object.get('type')}"')

    return set([item['name'] for item in notion_object['multi_select']])


with NotionApi(getenv('NOTION_API_KEY')) as notion:
    with RegulationsGovApi(getenv(key='REGULATIONS_GOV_API_KEY')) as regulations_gov:
        active_as_of_date = (datetime.now(tz=timezone.utc) - timedelta(days=7)).isoformat()
        rule_rows = notion.query_db(
            NOTION_RULE_DATABASE,
            {
                'or': [
                    {
                        'property': 'Comment End Date',
                        'date': {
                            'on_or_after': active_as_of_date
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
                                    'on_or_after': active_as_of_date
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
            fr_date = notion.cell_as_datetime(page['properties']['FR Publication Date'])
            print(f'{fr_number}: {fr_date}')

            old_comment_deadline_iso = page['properties'].get('Comment End Date')
            old_comment_deadline = notion.cell_as_datetime(old_comment_deadline_iso) if old_comment_deadline_iso else None

            old_docket_docs = parse_rich_text_list(page['properties']['Docket Documents'])
            old_dockets = parse_rich_text_list(page['properties']['Dockets'])

            time.sleep(REGULATIONS_GOV_REQUEST_INTERVAL)
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

            if set(old_docket_docs) != set(found_docs):
                print(f'  Docket Docs (Old): {sorted(old_docket_docs)}')
                print(f'              (New): {sorted(found_docs)}')
                updates['Docket Documents'] = {
                    'type': 'rich_text',
                    'rich_text': notion_rich_text_url_list(
                        (d, f'https://www.regulations.gov/document/{d}')
                        for d in sorted(found_docs)
                    )
                }
            if set(old_dockets) != set(found_dockets):
                print(f'  Dockets (Old): {sorted(old_dockets)}')
                print(f'          (New): {sorted(found_dockets)}')
                updates['Dockets'] = {
                    'type': 'rich_text',
                    'rich_text': notion_rich_text_url_list(
                        (d, f'https://www.regulations.gov/docket/{d}')
                        for d in sorted(found_dockets)
                    )
                }

            if ALWAYS_UPDATE_DOCKET_DATA or 'Dockets' in updates:
                # TODO: Use docket.attributes.docketType
                # This does not have a field in Notion (yet!).
                new_keywords = set()
                new_rins = set()
                for docket_id in found_dockets:
                    time.sleep(REGULATIONS_GOV_REQUEST_INTERVAL)
                    docket = Docket.from_api(regulations_gov.get_docket(docket_id))
                    new_keywords.update(docket.keywords)
                    if docket.rin:
                        new_rins.add(docket.rin)

                old_keywords = parse_multiselect_set(page['properties']['Docket Keywords'])
                if old_keywords != new_keywords:
                    print(f'  KW (old): {sorted(old_keywords)}')
                    print(f'     (new): {sorted(new_keywords)}')
                    updates['Docket Keywords'] = {
                        'type': 'multi_select',
                        'multi_select': [
                            {'name': keyword}
                            for keyword in sorted(new_keywords)
                        ]
                    }

                    fr_topics = parse_multiselect_set(page['properties']['FR Topics'])
                    updates['Tags'] = {
                        'type': 'multi_select',
                        'multi_select': [
                            {'name': tag}
                            for tag in sorted([*fr_topics, *new_keywords])
                        ]
                    }

                # RINs are a little complicated; they belong to Dockets, but
                # sometimes a document on regulations.gov does not have a user-
                # visible docket. The correct RINs are often listed on the
                # Federal Register, which we don't re-query here, so we only
                # add to the list of known RINs and never remove.
                old_rins = set(parse_rich_text_list(page['properties']['RINs']))
                # TODO: remove this old "Not Assigned" check after remediating
                # old data. These always should have been skipped and this is
                # here to help clear them out.
                if new_rins.difference(old_rins) or 'Not Assigned' in old_rins:
                    new_rins.update([
                        rin
                        for rin in old_rins
                        if rin.lower() != 'not assigned'
                    ])
                    print(f'  RIN (old): {sorted(old_rins)}')
                    print(f'      (new): {sorted(new_rins)}')
                    updates['RINs'] = notion_rich_text(', '.join(sorted(new_rins)))

            if old_comment_deadline != latest_comment_date:
                print(f'  New comment deadline: {latest_comment_date} (old: {old_comment_deadline})')
                updates['Comment End Date'] = {
                    'type': 'date',
                    'date': {
                        'start': latest_comment_date.isoformat()
                    } if latest_comment_date else None
                }

            if updates:
                # print(f'  Updates: {updates}')
                notion.update_page(page['id'], updates)
