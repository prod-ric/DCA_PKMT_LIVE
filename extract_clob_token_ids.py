import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional


def extract_market_info(json_file: Path) -> Optional[Dict[str, Any]]:
    """
    Extract market information from a JSON file
    
    Args:
        json_file: Path to the JSON file
        
    Returns:
        Dict with 'clobTokenIds', 'conditionId', 'title', 'question', 'startDate', 'endDate' if found, None otherwise
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        title = data.get('title')
        if not title:
            print(f"⚠️  {json_file.name}: No 'title' field found")
            return None
        
        start_date = data.get('startDate')
        end_date = data.get('endDate')

        markets = data.get('markets', [])
        if not markets:
            print(f"⚠️  {json_file.name}: No 'markets' array found")
            return None
        
        matching_market = None
        for market in markets:
            if market.get('question') == title:
                matching_market = market
                break
        
        if not matching_market:
            print(f"⚠️  {json_file.name}: No market found with question matching title '{title}'")
            return None
        
        condition_id = matching_market.get('conditionId')
        if not condition_id:
            print(f"⚠️  {json_file.name}: No 'conditionId' field found in matching market")
            return None
        
        clob_token_ids_str = matching_market.get('clobTokenIds')
        if not clob_token_ids_str:
            print(f"⚠️  {json_file.name}: No 'clobTokenIds' field found in matching market")
            return None
        
        try:
            clob_token_ids = json.loads(clob_token_ids_str)
            if len(clob_token_ids) != 2:
                print(f"⚠️  {json_file.name}: Expected 2 clobTokenIds, got {len(clob_token_ids)}")
                return None
            
            return {
                'clobTokenIds': clob_token_ids,
                'conditionId': condition_id,
                'title': title,
                'question': matching_market.get('question', title),
                'startDate': start_date,
                'endDate': end_date
            }
        except json.JSONDecodeError as e:
            print(f"⚠️  {json_file.name}: Error parsing clobTokenIds: {e}")
            return None
        
    except json.JSONDecodeError as e:
        print(f"❌ {json_file.name}: Error parsing JSON: {e}")
        return None
    except Exception as e:
        print(f"❌ {json_file.name}: Error reading file: {e}")
        return None


def extract_clob_token_ids(json_file: Path) -> Optional[List[str]]:
    info = extract_market_info(json_file)
    return info['clobTokenIds'] if info else None


def generate_markets_json_format(market_data: Dict[str, Dict[str, Any]], default_capital: float = 100.0) -> Dict[str, Any]:
    """
    Generate markets data in the format required by live/markets.json
    
    Args:
        market_data: Dictionary of market information extracted from JSON files
        default_capital: Default capital to assign to each market
        
    Returns:
        Dictionary in the format: {"markets": [...]}
    """
    markets_list = []
    
    for filename, info in sorted(market_data.items()):
        # Generate a simple name from the title (lowercase, replace spaces with hyphens)
        title = info.get('title', filename.replace('.json', ''))
        name = title.lower().replace(' ', '-').replace('?', '').replace(',', '')[:50]
        
        # Format dates to ISO 8601 format if available
        start_time = info.get('startDate')
        end_time = info.get('endDate')
        
        # Ensure dates end with 'Z' for UTC timezone
        if start_time and not start_time.endswith('Z'):
            start_time = start_time + 'Z' if 'T' in start_time else start_time
        if end_time and not end_time.endswith('Z'):
            end_time = end_time + 'Z' if 'T' in end_time else end_time
        
        market_entry = {
            "condition_id": info['conditionId'],
            "asset_ids": info['clobTokenIds'],
            "name": name,
            "start_time": start_time or "2026-02-10T00:00:00Z",
            "end_time": end_time or "2026-02-10T03:00:00Z",
            "capital": default_capital
        }
        
        markets_list.append(market_entry)
    
    return {"markets": markets_list}


def update_markets_json_file(market_data: Dict[str, Dict[str, Any]], output_path: str = "live/markets.json", default_capital: float = 100.0):
    """
    Update or create a markets.json file in the live/ directory
    
    Args:
        market_data: Dictionary of market information extracted from JSON files
        output_path: Path to the output markets.json file
        default_capital: Default capital to assign to each market
    """
    markets_json = generate_markets_json_format(market_data, default_capital)
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(markets_json, f, indent=4, ensure_ascii=False)
    
    print("\n" + "=" * 80)
    print(f"✓ Updated {output_path} with {len(markets_json['markets'])} markets")
    print("=" * 80)
    print("\nGenerated markets.json content:")
    print(json.dumps(markets_json, indent=4))


def update_config_file(market_data: Dict[str, Dict[str, Any]], config_path: str = "config.py"):
    config_file = Path(config_path)
    if not config_file.exists():
        print(f"⚠️  Config file '{config_path}' not found, skipping update")
        return
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config_content = f.read()
    
    markets_dict = {}
    for idx, (filename, info) in enumerate(sorted(market_data.items()), 1):
        market_key = f'market_{idx}'
        name = info.get('title', filename.replace('.json', ''))
        
        clob_ids = info['clobTokenIds']
        yes_asset = clob_ids[0]
        no_asset = clob_ids[1]
        
        markets_dict[market_key] = {
            'market_id': info['conditionId'],
            'name': name,
            'assets': {
                'YES': yes_asset,
                'NO': no_asset,
            },
            'startDate': info.get('startDate'),
            'endDate': info.get('endDate'),
        }
    
    markets_str = "MARKETS = {\n"
    for market_key, market in markets_dict.items():
        markets_str += f"    '{market_key}': {{\n"
        markets_str += f"        'market_id': '{market['market_id']}',\n"
        markets_str += f"        'name': {repr(market['name'])},\n"
        markets_str += "        'assets': {\n"
        markets_str += f"            'YES': '{market['assets']['YES']}',\n"
        markets_str += f"            'NO':  '{market['assets']['NO']}',\n"
        markets_str += "        },\n"
        markets_str += f"        'startDate': {repr(market['startDate'])},\n"
        markets_str += f"        'endDate': {repr(market['endDate'])},\n"
        markets_str += "    },\n"
    markets_str += "    # Add more markets as needed\n"
    markets_str += "}\n"
    
    lines = config_content.split('\n')
    new_lines = []
    skip_until_brace = False
    brace_count = 0
    found_markets = False
    comment_lines = []
    
    for i, line in enumerate(lines):
        if 'MARKETS = {' in line and not found_markets:
            j = i - 1
            while j >= 0 and (lines[j].strip().startswith('#') or lines[j].strip() == ''):
                comment_lines.insert(0, lines[j])
                j -= 1
            
            if comment_lines:
                new_lines.extend(comment_lines)
            
            new_lines.append(markets_str.rstrip())
            skip_until_brace = True
            brace_count = line.count('{') - line.count('}')
            found_markets = True
            continue
        elif skip_until_brace:
            brace_count += line.count('{') - line.count('}')
            if brace_count == 0:
                skip_until_brace = False
            continue
        else:
            new_lines.append(line)
    
    config_content = '\n'.join(new_lines)
    
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write(config_content)
    
    print("\n" + "=" * 80)
    print(f"✓ Updated {config_path} with {len(markets_dict)} markets")
    print("=" * 80)


def process_all_markets(markets_dir: str = "markets", output_file: str = "clob_token_ids_by_file.json"):
    markets_path = Path(markets_dir)
    print(markets_path)

    if not markets_path.exists():
        print(f"❌ Directory '{markets_dir}' does not exist")
        return

    json_files = sorted(markets_path.glob("*.json"))
    print(sorted(markets_path.glob("*")))

    if not json_files:
        print(f"⚠️  No JSON files found in '{markets_dir}' directory")
        return
    
    print(f"Found {len(json_files)} JSON files in '{markets_dir}'")
    print("=" * 80)
    print()
    
    results = {}
    market_data = {}
    
    for json_file in json_files:
        print(f"Processing: {json_file.name}")
        market_info = extract_market_info(json_file)
        
        if market_info:
            clob_token_ids = market_info['clobTokenIds']
            results[json_file.name] = {
                'clobTokenIds': clob_token_ids,
                'conditionId': market_info['conditionId'],
                'count': len(clob_token_ids)
            }
            market_data[json_file.name] = market_info

            print(f"  ✓ Found {len(clob_token_ids)} clobTokenIds: {clob_token_ids}")
            print(f"  ✓ Market ID: {market_info['conditionId']}")
            print(f"  ✓ YES/NO Assets: {clob_token_ids} (Condition ID: {market_info['conditionId']})")
            print(f"  ✓ Title: {market_info['title']}")
            print(f"  ✓ Start Date: {market_info.get('startDate', 'N/A')}")
            print(f"  ✓ End Date: {market_info.get('endDate', 'N/A')}")
        else:
            results[json_file.name] = None
            print(f"  ✗ No market info extracted")
        print()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("=" * 80)
    print(f"Results saved to: {output_file}")
    
    successful = sum(1 for v in results.values() if v is not None)
    failed = len(results) - successful
    
    print(f"\nSummary:")
    print(f"  Total files processed: {len(results)}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    
    print("\n" + "=" * 80)
    print("CLOB TOKEN IDs BY FILE (in order):")
    print("=" * 80)
    for filename, data in results.items():
        if data:
            print(f"{filename}: {data['clobTokenIds']} ({data['conditionId']}) \n ")
        else:
            print(f"{filename}: None")
    
    all_token_ids = []
    for data in results.values():
        if data and data.get('clobTokenIds'):
            all_token_ids.extend(data['clobTokenIds'])
    
    unique_token_ids = list(set(all_token_ids))
    
    print("\n" + "=" * 80)
    print("ALL UNIQUE CLOB TOKEN IDs:")
    print("=" * 80)
    print(unique_token_ids)
    print(f"\nTotal unique token IDs: {len(unique_token_ids)}")
    
    if market_data:
        update_config_file(market_data)
        update_markets_json_file(market_data, output_path="live/markets.json", default_capital=100.0)


if __name__ == "__main__":
    import sys
    markets_dir = "data_markets/"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "clob_token_ids_by_file.json"
    
    process_all_markets(markets_dir, output_file)