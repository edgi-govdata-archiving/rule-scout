from os import getenv
from rule_scout import (
    NOTION_RULE_DATABASE,
    Docket,
    FederalRegisterApi,
    NotionApi,
    RegulationsGovApi,
    load_fr_document,
    print_fr_document,
    save_fr_document_in_notion
)
import sys


def main() -> None:
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('document_id', help=(
        'The number/ID of the Federal Register document to import, e.g. '
        '"2026-13608"'
    ))
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update entry in Notion if it already exists.'
    )
    parser.add_argument(
        '--commit',
        action='store_true',
        help='Actually create new entries in Notion.'
    )
    args = parser.parse_args()

    document_id = args.document_id.strip()
    existing_row = None
    with NotionApi(getenv('NOTION_API_KEY')) as notion:
        rule_rows = notion.query_db(
            NOTION_RULE_DATABASE,
            {
                'property': 'FR Document Number',
                'rich_text': {
                    'equals': args.document_id,
                }
            }
        )
        matches = list(rule_rows)
        if len(matches) > 0:
            if len(matches) > 1:
                print(f'WARNING: found multiple entries ({len(matches)}) for this document!')
            if args.update:
                existing_row = matches[0]
                # print(matches[0])
                print('--update is not yet implemented!')
                sys.exit(1)
            else:
                print(f'Document {document_id} is already in Notion at: {matches[0]["url"]}')
                sys.exit(1)

        # TODO: consider how to better implement this. The easy thing is to put
        # @lru_cache on the RegulationsGovApi.get_docket method, but that could
        # introduce problems if the way we use it changes.
        docket_cache: dict[str, Docket] = {}
        with FederalRegisterApi() as register:
            with RegulationsGovApi(getenv(key='REGULATIONS_GOV_API_KEY')) as regulations_gov:
                data = load_fr_document(document_id, register, regulations_gov, docket_cache)
                print_fr_document(data)

                if args.commit:
                    save_fr_document_in_notion(data, notion)


if __name__ == "__main__":
    main()
