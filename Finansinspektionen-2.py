import bs4
import re
import requests
import datetime
import json
import os
import time
import argparse
import sys
 
### to add: if USD, reduce threshold to 50,000

display_name = "Finansinspektionen Insider Reports Monitor"

SEEN_REPORTS_FILE = "seen_reports.json"
URL = f"https://marknadssok.fi.se/Publiceringsklient/en-GB/Search/Search?SearchFunctionType=Insyn&Utgivare=&PersonILedandeStällningNamn=&Transaktionsdatum.From=&Transaktionsdatum.To=&Publiceringsdatum.From=&Publiceringsdatum.To=&button=search&Page=1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
}

def preview():
    return {
        "display_name": display_name,
        "script_type": "stream",
        "tags": [
            {"name": "obligated_name", "tip": "name of the person with obligation to disclose", "detail": "{obligated_name}: e.g. 'Stefan Persson'"},
            {"name": "position", "tip": "position of obligated person", "detail": "{position}: e.g. 'CEO', 'CFO', 'Board Member'"},
            {"name": "issuer", "tip": "name of the issuer", "detail": "{issuer}: e.g. Mendus AB"},
            {"name": "clean_instrument", "tip": "traded instrument name", "detail": "{clean_instrument}: e.g. Mendus, 'Common Stock (NYSE: ALV)'"},
            {"name": "transaction_text", "tip": "text of transaction type", "detail": "{transaction_text}: 'acquired' or 'disposed'"},
            {"name": "currency", "tip": "currency used in transaction", "detail": "{currency}: e.g. SEK, USD etc."},
            {"name": "total_value_str", "tip": "formatted total value", "detail": "{total_value_str}: e.g. '1.23 BLN', '12.3 MLN', '1,234', '123.45' etc."},
            {"name": "date_text", "tip": "date of transaction", "detail": "{comp_traffic}: July 1st, December 24th"},
            {"name": "transaction_place", "tip": "where transaction took place", "detail": "{traffic_change}: directly from the report, not formatted"},
        ],
        "template_sentences": [
        ],
        "monitor": [f"{URL}"],
        "fields": [
            {"name": "threshold", "tip": "total value in SEK threshold for filtering, default set to SEK 500,000"},
            {"name": "poll_interval", "tip": "polling interval in seconds, default set to 60 seconds"},
        ]
    }

seen_reports = set()
# def load_seen_reports():
#     if os.path.exists(SEEN_REPORTS_FILE):
#         try:
#             with open(SEEN_REPORTS_FILE, 'r') as f:
#                 return set(json.load(f))
#         except Exception as e:
#             print(f"Error loading seen reports: {e}")
#     return set()

# def save_seen_reports(seen_reports):
#     try:
#         with open(SEEN_REPORTS_FILE, 'w') as f:
#             json.dump(list(seen_reports), f)
#     except Exception as e:
#         print(f"Error saving seen reports: {e}")

def poll_website(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                return response.text
            else:
                print(f"Attempt {attempt + 1}: Received status code {response.status_code}")
                return ""  # Always return a string 
                
        except requests.RequestException as e:
            print(f"Error fetching website: {e}")

        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # 1, 2, 4, 8, etc. seconds
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    print("All attempts to fetch website failed.")
    return None

def poll_website_continuously(url, poll_interval, threshold):  
    try:
        while True:
            print(f"Polling website for new reports...")
            
            # Get the main page content
            main_page = poll_website(url)
            if not main_page:
                print(f"Failed to fetch main page. Will retry in {poll_interval}.")
                time.sleep(poll_interval)
                continue

            reports = parse_mainpage(main_page, threshold)
            
            new_reports = [report for report in reports if report not in seen_reports]
            
            if new_reports:
                processed_reports = parse_new_reports(new_reports)

                seen_reports.update(processed_reports)
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        print("Polling interrupted by user.")
    except Exception as e:
        print(f"Error in polling loop: {e}")
        raise

def parse_mainpage(main_page, threshold):
    seen_reports = set()
    soup = bs4.BeautifulSoup(main_page, "html.parser")
    table = soup.find("table", {"class": "table table-bordered table-hover table-striped zero-margin-top"})
    
    if not table:
        print("Warning: Could not find the table with reports")
        return []  # Always return a list

    cells = table.find_all("td")
    rows = table.find("tbody").find_all("tr")
    seen_reports = set()
    to_process = set()
    
    for row in rows:
        cells = row.find_all("td")
        # Check if we have enough cells
        if len(cells) >= 16:
            # publication_date = cells[0].text.strip() # publication date
            # issuer = cells[1].text.strip()  # This gets "Promimic AB"
            person = cells[2].text.strip()
            person_surname = person.split(" ")[-1].strip() if person else None
            first_surname_maybe = person.split(" ")[-2].strip() if person else None
            # position = cells[3].text.strip()  # "Chief Financial Officer (CFO)"
            closely_associated = cells[4].text.strip()
            transaction_nature = cells[5].text.strip()
            # instrument_name = cells[6].text.strip()  # "Promimic AB" (again)
            instrument_type = cells[7].text.strip()
            # isin = cells[8].text.strip()  # ISIN code
            # transaction_date = cells[9].text.strip()
            volume = cells[10].text.strip()  # Transaction volume
            unit = cells[11].text.strip()  # Unit (e.g., "Quantity")
            price = cells[12].text.strip()  # Price per unit
            currency = cells[13].text.strip()  # "SEK"
            status = cells[14].text.strip() 

            # Report link
            details_cell = cells[15] 
            details_link = details_cell.find("a", href=True)
            report_link = details_link["href"] if details_link else None

            # Calculate total value
            try:
                volume_float = float(volume.replace(",", "").strip())
                price_float = float(price.replace(",", "").strip())
                total_value = volume_float * price_float
            except ValueError:
                total_value = 0.0
            
            if(
                    instrument_type == "Share"
                    and total_value > threshold
                    and transaction_nature in ["Acquisition", "Disposal"]
                    and (closely_associated == ""
                    or person_surname in ["Arnhult", "Persson"] or first_surname_maybe == ["Arnhult", "Persson"])
                ):
                seen_reports.add(report_link)
                to_process.add(report_link)
            else:
                seen_reports.add(report_link)

    return to_process

def parse_new_reports(to_process):
    for report in to_process:
        # time.sleep(3) # speed brake
        report_index = report.split("/Index/")[1].split("?")[0]
        report_url = f"https://marknadssok.fi.se{report}"
        print(f"Checking inside the report: {report_index}")
        try:
            response = requests.get(report_url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            # Retry logic: try up to 3 times with a 2 second delay between attempts
            for attempt in range(3):
                if response.status_code == 200:
                    break
                else:
                    time.sleep(2)
                    try:
                        response = requests.get(report_url, headers=HEADERS, timeout=10)
                    except requests.RequestException:
                        continue
            if response.status_code == 200:
                soup = bs4.BeautifulSoup(response.text, "html.parser")
                panels = soup.find_all("div", {"class": "panel panel-default"})
                obligated_name_div = soup.find("div", {"class": "col-sm-4 text-right"}, 
                                        string=re.compile(r"\s*Name of person with notification obligation\s*", re.IGNORECASE))
                obligated_name = obligated_name_div.find_next_sibling().text.strip() if obligated_name_div else None

                obligated_name_lower = obligated_name.lower() if obligated_name else ""
                for suffix in [" ab", "abp", "hb", "kb", " ab (publ.)"]:
                    if obligated_name_lower.endswith(suffix):
                        obligated_clean_name = obligated_name_lower[: -len(suffix)].strip()
                        break
                else:
                    obligated_clean_name = obligated_name_lower
                
                closely_div = soup.find("div", {"class": "col-sm-4 text-right"}, 
                                string=re.compile(r"\s*Closely associated\s*", re.IGNORECASE))
                isClose = closely_div.find_next_sibling("div").text.strip() == "Yes" if closely_div and closely_div.find_next_sibling("div") else False
                
                managerial_div = soup.find("div", {"class": "col-sm-4 text-right"}, 
                                    string=re.compile(r"\s*Person discharging managerial responsibilities\s*", re.IGNORECASE))
                managerial_person = managerial_div.find_next_sibling("div").text.strip() if managerial_div else None
                manager_surname = managerial_person.split(" ")[-1].strip() if managerial_person else None
                
                position_div = soup.find("div", {"class": "col-sm-4 text-right"}, 
                                string=re.compile(r"\s*Position\s*", re.IGNORECASE))
                position_full = position_div.find_next_sibling("div") if position_div else None

                initial_div = soup.find("div", {"class": "col-sm-4 text-right"},
                                string=re.compile(r"\s*Initial notification\s*", re.IGNORECASE))
                isInitial = initial_div.find_next_sibling("div").text.strip() == "Yes" if closely_div and closely_div.find_next_sibling("div") else False

                issuer_div = soup.find("div", {"class": "col-sm-4 text-right"}, 
                                string=re.compile(r"\s*Name of issuer\s*", re.IGNORECASE))
                issuer = issuer_div.find_next_sibling("div").text.strip() if issuer_div else None

                issuer_lower = issuer.lower()
                issuer_clean = issuer
                for suffix in [" ab", "abp", "hb", "kb", " ab (publ.)", " ab (publ)", "(publ)", "(publ.)", " ab (publ.)"]:
                    if issuer_lower.endswith(suffix.lower()):
                        issuer_clean = issuer[: -len(suffix)].strip()
                        if issuer_clean.lower() == "h & m hennes & mauritz":
                            issuer_clean = "H&M"
                        else:
                            issuer_clean = issuer_clean.title()
                        break
                else:
                    # No suffix found, use title case of original
                    issuer_clean = issuer.title()


                if position_full:
                    position_text = position_full.text.strip().lower()
                    match position_text:
                        case "chief executive officer (ceo)/managing directory":
                            position = "CEO"
                        case "chief operating officer (coo)":
                            position = "COO"
                        case "chief technology officer (cto)":
                            position = "CTO"
                        case "Chief financial officer (cfo)":
                            position = "CFO"
                        case "member of the board of directors":
                            position = "Board Member"
                        case "chairman of the board of directors":
                            position = "Chairman of the Board"
                        case "other senior executive":
                            position = "Senior Executive"
                        case "member of the supervisory board":
                            position = "Supervisory Board Member"
                        case "other member of the company's administrative, management or supervisory body":                    
                            position = "Executive"
                        case _:
                            position = position_text
                            pass

                if position_full:
                    position_text = position_full.text.strip().lower()
                    match position_text:
                        case "chief executive officer (ceo)/managing directory":
                            position_combined = f"CEO OF {issuer_clean}"
                        case "chief operating officer (coo)":
                            position_combined = f"COO OF {issuer_clean}"
                        case "chief technology officer (cto)":
                            position_combined = f"CTO OF {issuer_clean}"
                        case "Chief financial officer (cfo)":
                            position_combined = f"CFO"
                        case "member of the board of directors":
                            position_combined = f"Board Member of {issuer_clean}"
                        case "chairman of the board of directors":
                            position_combined = f"Chairman of {issuer_clean} Board"
                        case "other senior executive":
                            position_combined = f"Senior Executive of {issuer_clean}"
                        case "member of the supervisory board":
                            position_combined = f"Supervisory Board Member of {issuer_clean}"
                        case "other member of the company's administrative, management or supervisory body":                    
                            position_combined = f"Executive of {issuer_clean}"
                        case _:
                            position_combined = position_text
                            pass                
                

            else:
                continue

            
            # print("-------------- meta information --------------")
            # print(f"obligated_name: {obligated_name if obligated_name else 'Not found'}")
            # print(f"Is closely associated: {isClose}")
            # print(f"Managerial person: {managerial_person if managerial_person else 'Not found'}")
            # print(f"Position: {position if position else 'Not found'}")
            # print(f"Issuer: {issuer if issuer else 'Not found'}")
            # print("-------------- ---------------- --------------")

            # transaction details
            transaction_table = soup.find("table", {"class": "table table-bordered table-hover table-striped"})
            rows = transaction_table.find("tbody").find_all("tr")
            transaction_results = []  # Move this OUTSIDE the transaction loop
            for row in rows:
                cells = row.find_all("td")
                instrument_type = cells[0].text.strip()
                instrument = cells[1].text.strip()

                #clean instrument name
                clean_instrument = instrument.split(",")[0].strip()
                if clean_instrument.endswith(" AB"):
                    clean_instrument = clean_instrument[:-3].strip()

                transaction = cells[3].text.strip()
                isShareOption = (row.find("input", {"name": re.compile(r"ÄrKoppladTillAktieoptionsprogram\d+")}) or {}).get("value", "").lower() == "true"
                volume = cells[5].text.strip()
                price_pu = cells[7].text.strip()
                currency = cells[8].text.strip()
                transaction_date = cells[9].text.strip()
                transaction_place = cells[10].text.strip()

                # dates
                try:
                    dt = datetime.datetime.strptime(transaction_date, "%d/%m/%Y")
                    day = dt.day
                    if 4 <= day <= 20 or 24 <= day <= 30:
                        suffix = "th"
                    else:
                        suffix = ["st", "nd", "rd"][day % 10 - 1]
                    date_text = f"{dt.strftime('%B')} {day}{suffix}"
                except Exception:
                    date_text = transaction_date

                # Calculate total value
                volume_clean = volume.replace(",", "").strip() if volume else "0"
                price_pu_clean = price_pu.replace(",", "").strip() if price_pu else "0"
                try:
                    volume_float = float(volume_clean)
                except ValueError:
                    volume_float = 0.0
                try:
                    price_pu_float = float(price_pu_clean)
                except ValueError:
                    price_pu_float = 0.0
                total_value = volume_float * price_pu_float if volume_float and price_pu_float else 0.0

                # format total_value as e.g. 12.3 MLN
                if total_value >= 1_000_000_000:
                    total_value_str = f"{total_value/1_000_000_000:.1f} BLN"
                elif total_value >= 1_000_000:
                    total_value_str = f"{total_value/1_000_000:.1f} MLN"
                elif total_value >= 1_000:
                    total_value_str = f"{total_value:,.0f}"
                else:
                    total_value_str = f"{total_value:.0f}"

                match transaction:
                    case "Acquisition":
                        transaction_text = "acquired"
                    case "Disposal":
                        transaction_text = "disposed"
                    case _:
                        transaction_text = transaction

                # print("-------------- transaction details --------------")
                # print(f"Instrument type: {instrument_type}")
                # print(f"Instrument: {instrument}")
                # print(f"Transaction: {transaction}")
                # print(f"Is Share Option: {isShareOption}")
                # print(f"Volume: {volume}")
                # print(f"Price per unit: {price_pu}")
                
                # print(f"Total transaction value: {total_value}")
                # print(f"Currency: {currency}")
                # print(f"Transaction place: {transaction_place}")
                # print("-------------- ------------------ --------------")

                transaction_result = {
                    "obligated_name": obligated_name.upper(),
                    "obligated_clean_name": obligated_clean_name.upper(),
                    "managerial_person": managerial_person.upper(),
                    # "position": position.upper(),
                    "issuer": issuer.upper(),
                    "issuer_clean": issuer_clean.upper(),
                    "position_combined": position_combined.upper(),
                    "clean_instrument": clean_instrument.upper(),
                    "transaction": transaction.upper(),
                    "transaction_text": transaction_text.upper(),
                    "currency": currency.upper(),
                    "volume": volume_clean.upper(),
                    "total_value": total_value,
                    "total_value_str": total_value_str.upper(),
                    "date_text": date_text.upper(),
                    "transaction_place": transaction_place.upper(),
                    "isClose": isClose,
                }

                transaction_results.append(transaction_result)

            output_sentences = []
            for transaction_result in transaction_results:
                transaction = transaction_result.get('transaction')
                transaction_place = transaction_result.get('transaction_place')
                total_value = transaction_result.get('total_value', 0)
                if transaction in ["ACQUISITION", "DISPOSAL"] and transaction_place not in ["OUTSIDE A TRADING VENUE"]:
                    if total_value > 0:
                        if transaction_result.get('isClose') == False:
                            transaction_sentence = (
                                f"{transaction_result.get('position_combined')} {transaction_result.get('managerial_person')} "
                                f"{transaction_result.get('transaction_text')} {transaction_result.get('currency')} "
                                f"{transaction_result.get('total_value_str')} WORTH OF CO'S "
                                f"SHARES ON {transaction_result.get('date_text')}-FINANSINSPEKTIONEN"                      
                            )
                            place_sentence = f"TRANSACTION PLACE: {transaction_result.get('transaction_place')}"
                            output_sentences.append(transaction_sentence)
                            output_sentences.append(place_sentence)
                        else:
                            transaction_sentence = (
                                f"{transaction_result.get('obligated_clean_name')}, CLOSELY ASSOCIATED WITH "
                                f"{transaction_result.get('position_combined')} {transaction_result.get('managerial_person')}, "
                                f"{transaction_result.get('transaction_text')} {transaction_result.get('currency')} "
                                f"{transaction_result.get('total_value_str')} WORTH OF CO'S "
                                f"SHARES ON {transaction_result.get('date_text')}-FINANSINSPEKTIONEN"
                            )
                            place_sentence = f"TRANSACTION PLACE: {transaction_result.get('transaction_place')}"
                            output_sentences.append(transaction_sentence)
                            output_sentences.append(place_sentence)
                    elif total_value == 0:
                        if transaction_result.get('isClose') == False:
                            transaction_sentence = (
                                f"{transaction_result.get('position_combined')} {transaction_result.get('managerial_person')} "
                                f"{transaction_result.get('transaction_text')} {transaction_result.get('volume')} CO'S"
                                f"SHARES ON {transaction_result.get('date_text')}-FINANSINSPEKTIONEN"                   
                            )
                            place_sentence = f"TRANSACTION PLACE: {transaction_result.get('transaction_place')}"
                            output_sentences.append(transaction_sentence)
                            output_sentences.append(place_sentence)
                        else:
                            transaction_sentence = (
                                f"{transaction_result.get('obligated_clean_name')}, CLOSELY ASSOCIATED WITH "
                                f"{transaction_result.get('position_combined')} {transaction_result.get('managerial_person')}, "
                                f"{transaction_result.get('transaction_text')} {transaction_result.get('volume')} OF CO'S"
                                f"SHARES ON {transaction_result.get('date_text')}-FINANSINSPEKTIONEN"    
                            )
                            place_sentence = f"TRANSACTION PLACE: {transaction_result.get('transaction_place')}"
                            output_sentences.append(transaction_sentence)
                            output_sentences.append(place_sentence)

            if output_sentences:
                found = {"found": f"{report_url}"}
                print(json.dumps(found))
                print(json.dumps({"output_sentences": output_sentences}))
                print(f"Reports processed successfully...")
            else:
                print(f"No relevant transactions found in report {report_index}")
                continue


            #aggregate details:
            # aggr_panel = soup.find("div", class_="panel-heading", 
            #             string=re.compile(r"\s*Aggregation\s*", re.IGNORECASE))
            # aggr_table = aggr_panel.find_next("table") if aggr_panel else None
            
            # if aggr_table and aggr_table.find("tbody"):
            #     aggr_rows = aggr_table.find("tbody").find_all("tr")
            #     for aggr_row in aggr_rows:
            #         aggr_cells = aggr_row.find_all("td")

            #         aggr_fin_instrument = aggr_cells[0].text.strip()
            #         aggr_isin = aggr_cells[1].text.strip()
            #         aggr_transaction = aggr_cells[2].text.strip()
            #         aggr_date = aggr_cells[3].text.strip()
            #         aggr_place = aggr_cells[4].text.strip()
            #         aggr_volume = aggr_cells[5].text.strip()
            #         aggr_price_pu = aggr_cells[6].text.strip()

            #         aggr_volume_clean = aggr_volume.replace("(Quantity)", "").replace(",", "").strip() if aggr_volume else "0"
            #         aggr_price_pu_clean = aggr_price_pu.replace("SEK", "").replace(",", "").strip() if aggr_price_pu else "0"
                    
            #         try:
            #             aggr_volume_float = float(aggr_volume_clean)
            #             aggr_price_pu_float = float(aggr_price_pu_clean)
            #             aggr_total_value = aggr_volume_float * aggr_price_pu_float
            #         except ValueError:
            #             aggr_volume_float = 0.0
            #             aggr_price_pu_float = 0.0
            #             aggr_total_value = 0.0

            # print("-------------- aggregate details --------------")
            # print(f"Aggregate financial instrument: {aggr_fin_instrument}")
            # print(f"Aggregate transaction: {aggr_transaction}")
            # print(f"Aggregate place: {aggr_place}")
            # print(f"Aggregate volume: {aggr_volume}")
            # print(f"Aggregate price per unit: {aggr_price_pu}")
            # print(f"Aggregate total value: {aggr_total_value:.2f}")
            # print("-------------- ------------------ --------------")
                
        
        except requests.RequestException as e:
            print(f"Error fetching report {report}: {e}")


def run(threshold, poll_interval):
    while True:
        try:
            print(f"Polling website for new reports...")
            main_page = poll_website(URL)
            if not main_page:
                print(f"Failed to fetch main page. Will retry in {poll_interval}.")
                time.sleep(poll_interval)
                continue

            to_process = parse_mainpage(main_page, threshold)
            print(f"Waiting for next polling cycle")
            new_reports = [report for report in to_process if report not in seen_reports]
            if new_reports:
                parse_new_reports(new_reports)
                print(f"Waiting for next report")
                seen_reports.update(new_reports)
            print(f"Waiting for next polling cycle")
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            print("Polling interrupted by user.")
            break
        except Exception as e:
            print(f"Error in polling loop: {e}")
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Finansinspektionen Insider Reports")
    parser.add_argument("--preview", action="store_true", help="Run the script in preview mode")
    parser.add_argument("--threshold", type=str, help="Custom threshold, default is 500,000 regardless of currency")
    parser.add_argument("--poll_interval", type=int, default=60, help="Polling interval in seconds")

    if len(sys.argv) > 1 and sys.argv[1] == "--preview":
        result = preview()
        print(json.dumps(result))

    else:
        threshold = parser.parse_args().threshold
        poll_interval = parser.parse_args().poll_interval
        args = parser.parse_args()
        try: 
            run(
                threshold=int(threshold) if isinstance(threshold, str) and threshold.isdigit() else 500000,
                poll_interval=int(poll_interval) if isinstance(poll_interval, str) and poll_interval.isdigit() else 60
            )
        except Exception as e:
            print(f"Error running the script: {e}")
