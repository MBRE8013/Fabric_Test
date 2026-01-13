# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Fabric Notebook: Connecteam Shifts API Structure Explorer
# Purpose: Explore notes, breaks, and shiftLayers nested structures
# Author: Mike Brents, VP Business Analytics - IntellaTriage
# Date: 2025-11-24

import requests
import json
from datetime import datetime, timedelta
from notebookutils import mssparkutils

# =============================================================================
# CONFIGURATION
# =============================================================================

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://itt-dataanalytics.vault.azure.net/"
SECRET_NAME = "ConnecteamKeyMBrents"

# Retrieve API token from Key Vault
print("Retrieving Connecteam API token from Azure Key Vault...")
try:
    API_TOKEN = mssparkutils.credentials.getSecret(KEY_VAULT_URL, SECRET_NAME)
    print("✓ Successfully retrieved API token\n")
except Exception as e:
    print(f"✗ Error retrieving API token: {str(e)}")
    raise

# Scheduler to explore (Triage Schedule - your main one)
SCHEDULER_ID = "10713608"
SCHEDULER_NAME = "Triage Schedule"

# Date range: Last 30 days to capture more variety
NOW = datetime.now()
START_DATE = NOW - timedelta(days=30)
END_DATE = NOW + timedelta(days=14)
START_TIME_UNIX = int(START_DATE.timestamp())
END_TIME_UNIX = int(END_DATE.timestamp())

BASE_URL = f"https://api.connecteam.com/scheduler/v1/schedulers/{SCHEDULER_ID}/shifts"

print("="*80)
print(f"EXPLORING NESTED STRUCTURES IN: {SCHEDULER_NAME}")
print("="*80)
print(f"Date range: {START_DATE:%Y-%m-%d} to {END_DATE:%Y-%m-%d}")
print(f"Looking for: notes, breaks, shiftDetails.shiftLayers")
print("="*80 + "\n")

# =============================================================================
# FETCH DATA
# =============================================================================

def fetch_all_shifts(scheduler_id, start_time, end_time, api_token):
    """Fetch all shifts for analysis"""
    url = f"https://api.connecteam.com/scheduler/v1/schedulers/{scheduler_id}/shifts"
    headers = {"X-API-Key": api_token, "Content-Type": "application/json"}
    all_shifts = []
    offset = 0
    limit = 500
    
    print(f"Fetching shifts from API...")
    
    while True:
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "sort": "created_at",
            "order": "asc",
            "limit": limit,
            "offset": offset
        }
        
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            shifts = data.get("data", {}).get("shifts", [])
            if not shifts:
                break
                
            all_shifts.extend(shifts)
            print(f"  Fetched {len(shifts)} shifts (Total: {len(all_shifts)})")
            
            if len(shifts) < limit:
                break
                
            offset += limit
            
        except Exception as e:
            print(f"ERROR: {e}")
            break
    
    print(f"✓ Total shifts retrieved: {len(all_shifts)}\n")
    return all_shifts

# =============================================================================
# ANALYZE NESTED STRUCTURES
# =============================================================================

def analyze_nested_structures(shifts):
    """Analyze notes, breaks, and shiftLayers"""
    
    print("="*80)
    print("ANALYZING NESTED STRUCTURES")
    print("="*80 + "\n")
    
    # Tracking
    shifts_with_notes = []
    shifts_with_breaks = []
    shifts_with_layers = []
    
    all_note_samples = []
    all_break_samples = []
    all_layer_samples = []
    
    # Analyze each shift
    for shift in shifts:
        shift_id = shift.get('id')
        shift_title = shift.get('title', 'No Title')
        shift_start = datetime.fromtimestamp(shift.get('startTime', 0))
        
        # Check notes
        notes = shift.get('notes', [])
        if notes and len(notes) > 0:
            shifts_with_notes.append(shift_id)
            for note in notes[:2]:  # Sample first 2 notes
                all_note_samples.append({
                    'shift_id': shift_id,
                    'shift_title': shift_title,
                    'shift_start': shift_start.strftime('%Y-%m-%d %H:%M'),
                    'note': note
                })
        
        # Check breaks
        breaks = shift.get('breaks', [])
        if breaks and len(breaks) > 0:
            shifts_with_breaks.append(shift_id)
            for brk in breaks[:2]:  # Sample first 2 breaks
                all_break_samples.append({
                    'shift_id': shift_id,
                    'shift_title': shift_title,
                    'shift_start': shift_start.strftime('%Y-%m-%d %H:%M'),
                    'break': brk
                })
        
        # Check shift layers
        shift_details = shift.get('shiftDetails', {})
        layers = shift_details.get('shiftLayers', [])
        if layers and len(layers) > 0:
            shifts_with_layers.append(shift_id)
            for layer in layers[:2]:  # Sample first 2 layers
                all_layer_samples.append({
                    'shift_id': shift_id,
                    'shift_title': shift_title,
                    'shift_start': shift_start.strftime('%Y-%m-%d %H:%M'),
                    'layer': layer
                })
    
    # Print summary
    total_shifts = len(shifts)
    print(f"SUMMARY:")
    print(f"  Total shifts analyzed: {total_shifts}")
    print(f"  Shifts with NOTES: {len(shifts_with_notes)} ({len(shifts_with_notes)/total_shifts*100:.1f}%)")
    print(f"  Shifts with BREAKS: {len(shifts_with_breaks)} ({len(shifts_with_breaks)/total_shifts*100:.1f}%)")
    print(f"  Shifts with SHIFT LAYERS: {len(shifts_with_layers)} ({len(shifts_with_layers)/total_shifts*100:.1f}%)")
    print("\n" + "="*80 + "\n")
    
    # Print detailed samples
    if all_note_samples:
        print("NOTES STRUCTURE (Sample):")
        print("-" * 80)
        for i, sample in enumerate(all_note_samples[:3], 1):
            print(f"\nSample {i}:")
            print(f"  Shift: {sample['shift_id']} - {sample['shift_title']}")
            print(f"  Start: {sample['shift_start']}")
            print(f"  Note Data:")
            print(json.dumps(sample['note'], indent=4))
        print("\n" + "="*80 + "\n")
    else:
        print("⚠ NO NOTES FOUND in dataset\n")
    
    if all_break_samples:
        print("BREAKS STRUCTURE (Sample):")
        print("-" * 80)
        for i, sample in enumerate(all_break_samples[:3], 1):
            print(f"\nSample {i}:")
            print(f"  Shift: {sample['shift_id']} - {sample['shift_title']}")
            print(f"  Start: {sample['shift_start']}")
            print(f"  Break Data:")
            print(json.dumps(sample['break'], indent=4))
        print("\n" + "="*80 + "\n")
    else:
        print("⚠ NO BREAKS FOUND in dataset\n")
    
    if all_layer_samples:
        print("SHIFT LAYERS STRUCTURE (Sample):")
        print("-" * 80)
        for i, sample in enumerate(all_layer_samples[:3], 1):
            print(f"\nSample {i}:")
            print(f"  Shift: {sample['shift_id']} - {sample['shift_title']}")
            print(f"  Start: {sample['shift_start']}")
            print(f"  Layer Data:")
            print(json.dumps(sample['layer'], indent=4))
        print("\n" + "="*80 + "\n")
    else:
        print("⚠ NO SHIFT LAYERS FOUND in dataset\n")
    
    # Return for further analysis if needed
    return {
        'note_samples': all_note_samples,
        'break_samples': all_break_samples,
        'layer_samples': all_layer_samples,
        'stats': {
            'total_shifts': total_shifts,
            'with_notes': len(shifts_with_notes),
            'with_breaks': len(shifts_with_breaks),
            'with_layers': len(shifts_with_layers)
        }
    }

# =============================================================================
# MAIN EXECUTION
# =============================================================================

# Fetch the data
shifts_data = fetch_all_shifts(SCHEDULER_ID, START_TIME_UNIX, END_TIME_UNIX, API_TOKEN)

if not shifts_data:
    print("No shifts found in the specified date range.")
else:
    # Analyze nested structures
    results = analyze_nested_structures(shifts_data)
    
    # Final recommendation
    print("="*80)
    print("RECOMMENDATION")
    print("="*80)
    
    stats = results['stats']
    
    if stats['with_notes'] > 0:
        print(f"✓ NOTES: Found in {stats['with_notes']} shifts")
        print("  → Consider creating 'connecteam_shift_notes' table")
        print("  → Fields likely: shift_id, note_id, note_text, created_by, created_time")
    else:
        print("✗ NOTES: Not used in your shifts")
    
    print()
    
    if stats['with_breaks'] > 0:
        print(f"✓ BREAKS: Found in {stats['with_breaks']} shifts")
        print("  → Consider creating 'connecteam_shift_breaks' table")
        print("  → Fields likely: shift_id, break_id, start_time, end_time, duration, is_paid")
    else:
        print("✗ BREAKS: Not used in your shifts")
    
    print()
    
    if stats['with_layers'] > 0:
        print(f"✓ SHIFT LAYERS: Found in {stats['with_layers']} shifts")
        print("  → Consider creating 'connecteam_shift_layers' table")
        print("  → Need to see structure to determine fields")
    else:
        print("✗ SHIFT LAYERS: Not used in your shifts")
    
    print("="*80)

print("\n✓ Analysis complete!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
