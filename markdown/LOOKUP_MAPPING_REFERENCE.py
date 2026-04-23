# FaRDaP Lookup Mapping Reference
# 
# This file documents how silver table columns connect to lookup tables.
# Pattern: silver_column contains a CODE (like "FA01") that matches lookup_table.id
#
# The lookup_table.name field contains the human-readable description.
# The lookup_table.guidance field contains additional context.

# ============================================================================
# MAPPING RULES
# ============================================================================
#
# RULE 1: Columns ending in "_value" are lookup codes
#   content_incidentatcall_mobiliseincidenttype_value -> lu_mobiliseincidenttype
#
# RULE 2: The lookup table name is derived from the column structure
#   content_{path}_{typename}_value -> lu_{typename}type
#
# RULE 3: All lookups join on the 'id' column
#   silver_table.column = lookup_table.id

LOOKUP_MAPPINGS = {
    # ========================================================================
    # INCIDENT CORE FIELDS
    # ========================================================================
    
    # Incident identification
    "fardap_silver_incidents.content_identifier_nationalstatisticsnumber_frsid": {
        "lookup_table": "lu_frsidlisttype",
        "description": "Fire & Rescue Service identifier"
    },
    
    # Incident at call
    "fardap_silver_incidents.content_incidentatcall_mobiliseincidenttype_value": {
        "lookup_table": "lu_mobiliseincidenttype",
        "description": "Type of incident as reported at call"
    },
    "fardap_silver_incidents.content_incidentatcall_mobiliseincidenttype_generictype": {
        "lookup_table": "lu_genericincidenttype",
        "description": "Generic incident category"
    },
    "fardap_silver_incidents.content_incidentatcall_originofcall_value": {
        "lookup_table": "lu_originofcalltype",
        "description": "How the call was received"
    },
    
    # Audit
    "fardap_silver_incidents.content_auditdetail_incidentstatus": {
        "lookup_table": "lu_incidentstatustype",
        "description": "Current status of incident record"
    },
    
    # ========================================================================
    # PROPERTY & LOCATION
    # ========================================================================
    "fardap_silver_incidents.content_incidentlocation_propertycategory_value": {
        "lookup_table": "lu_propertycategorytype",
        "description": "Category of property"
    },
    "fardap_silver_incidents.content_incidentlocation_propertytype_value": {
        "lookup_table": "lu_propertytype",
        "description": "Specific property type"
    },
    "fardap_silver_incidents.content_incidentlocation_isaddressable_value": {
        "lookup_table": "lu_isaddressabletype",
        "description": "Whether location has a postal address"
    },
    
    # ========================================================================
    # FALSE ALARM
    # ========================================================================
    "fardap_silver_incidents.content_incidentonattendance_falsealarm_falsealarmreason_value": {
        "lookup_table": "lu_falsealarmreasontype",
        "description": "Reason for false alarm"
    },
    
    # ========================================================================
    # FIRE DETAILS
    # ========================================================================
    "fardap_silver_incidents.content_incidentonattendance_fire_cause": {
        "lookup_table": "lu_firecausetype",
        "description": "Accidental/Deliberate/Unknown"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_building_normaloccupationstatus": {
        "lookup_table": "lu_buildingoccupationstatustype",
        "description": "Normal occupation status of building"
    },
    
    # Primary fire details
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_causeandreason_maincause_value": {
        "lookup_table": "lu_pfmaincausetype",
        "description": "Main cause of primary fire"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_causeandreason_ignitionsource_value": {
        "lookup_table": "lu_pfignitionsourcetype",
        "description": "Source of ignition"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_causeandreason_ignitionsourcepower_value": {
        "lookup_table": "lu_pfignitionsourcepowertype",
        "description": "Power source for ignition"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_causeandreason_itemfirstignited_value": {
        "lookup_table": "lu_pfitemignitedtype",
        "description": "Item first ignited"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_causeandreason_causedby_value": {
        "lookup_table": "lu_pfcausedbytype",
        "description": "What caused the fire"
    },
    
    # Fire damage
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_damage_damagerestrictedto_value": {
        "lookup_table": "lu_pffiresizetype",
        "description": "Extent of fire damage"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_damage_firesizeonarrival_value": {
        "lookup_table": "lu_pffiresizetype",
        "description": "Fire size when FRS arrived"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_outdoorarea_damagearea_value": {
        "lookup_table": "lu_damagearearangetype",
        "description": "Area of damage range"
    },
    
    # Fire start location (varies by property type)
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_dwelling_firestartlocation_value": {
        "lookup_table": "lu_pffirestartlocationtype",
        "description": "Where fire started in dwelling"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_nonresidential_firestartlocation_value": {
        "lookup_table": "lu_pffirestartlocationtype",
        "description": "Where fire started in non-residential"
    },
    
    # Discovery
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_discovery_howdiscovered_value": {
        "lookup_table": "lu_pfhowdiscoveredtype",
        "description": "How fire was discovered"
    },
    
    # ========================================================================
    # SAFETY SYSTEMS (in dwelling/nonresidential/otherresidential)
    # ========================================================================
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_dwelling_safetysystems_compartmentation_value": {
        "lookup_table": "lu_compartmentationtype",
        "description": "Compartmentation effectiveness"
    },
    "fardap_silver_incidents.content_incidentonattendance_fire_primaryfire_dwelling_safetysystems_meansofscape_value": {
        "lookup_table": "lu_meansofescapetype",
        "description": "Means of escape effectiveness"
    },
    
    # ========================================================================
    # SPECIAL SERVICES
    # ========================================================================
    "fardap_silver_incidents.content_incidentonattendance_specialservice_specialservicetype_value": {
        "lookup_table": "lu_specialservicetype",
        "description": "Type of special service"
    },
    
    # ========================================================================
    # VICTIM TABLE
    # ========================================================================
    "fardap_silver_victim.content_ethnicity_value": {
        "lookup_table": "lu_ethnicitytype",
        "description": "Victim ethnicity"
    },
    "fardap_silver_victim.content_gender_value": {
        "lookup_table": "lu_gendercurrenttype",
        "description": "Victim gender"
    },
    "fardap_silver_victim.content_agerange": {
        "lookup_table": "lu_agerangetype",
        "description": "Victim age range"
    },
    "fardap_silver_victim.content_injury_severity_value": {
        "lookup_table": "lu_severityofinjurytype",
        "description": "Severity of injury"
    },
    "fardap_silver_victim.content_injury_natureofinjury_value": {
        "lookup_table": "lu_natureofinjurytype",
        "description": "Nature of injury"
    },
    "fardap_silver_victim.content_role_value": {
        "lookup_table": "lu_victimroletype",
        "description": "Role of victim (occupant, firefighter, etc.)"
    },
    "fardap_silver_victim.content_causeofdeath_value": {
        "lookup_table": "lu_causeofdeathtype",
        "description": "Cause of death"
    },
    "fardap_silver_victim.content_fatalitycircumstances_value": {
        "lookup_table": "lu_fatalitycircumstancestype",
        "description": "Circumstances of fatality"
    },
    "fardap_silver_victim.content_locationatfirestart_value": {
        "lookup_table": "lu_locationatfirestarttype",
        "description": "Location of victim when fire started"
    },
    "fardap_silver_victim.content_humanfactors_value": {
        "lookup_table": "lu_humanfactorstype",
        "description": "Human factors affecting victim"
    },
    "fardap_silver_victim.content_rescuedby_value": {
        "lookup_table": "lu_rescuedbytype",
        "description": "Who rescued the victim"
    },
    "fardap_silver_victim.content_wherefound_value": {
        "lookup_table": "lu_wherefoundtype",
        "description": "Where victim was found"
    },
    
    # ========================================================================
    # VEHICLE TABLE
    # ========================================================================
    "fardap_silver_vehicle.content_vehicletype_value": {
        "lookup_table": "lu_vehicletype",
        "description": "Type of vehicle"
    },
    "fardap_silver_vehicle.content_extrication_methodused_value": {
        "lookup_table": "lu_extricationmethodusedtype",
        "description": "Method used for extrication"
    },
    "fardap_silver_vehicle.content_extrication_timetaken_value": {
        "lookup_table": "lu_extricationtimetakentype",
        "description": "Time taken for extrication"
    },
    "fardap_silver_vehicle.content_extrication_vehicleposition_value": {
        "lookup_table": "lu_extricationvehiclepositiontype",
        "description": "Vehicle position during extrication"
    },
    
    # ========================================================================
    # EQUIPMENT TABLE
    # ========================================================================
    "fardap_silver_equipment.content_equipmenttype_value": {
        "lookup_table": "lu_equipmenttype",
        "description": "Type of equipment"
    },
    "fardap_silver_equipment.content_equipmentcategory_value": {
        "lookup_table": "lu_equipmentcategorytype",
        "description": "Category of equipment"
    },
    
    # ========================================================================
    # HAZARDOUS MATERIAL TABLE
    # ========================================================================
    "fardap_silver_hazardousmaterial.content_unnumber_value": {
        "lookup_table": "lu_commonunnumberlisttype",
        "description": "UN number for hazardous material"
    },
    
    # ========================================================================
    # ADDITIONAL INFO TABLE
    # ========================================================================
    "fardap_silver_additionalinfo.content_infotype_value": {
        "lookup_table": "lu_infotype",
        "description": "Type of additional information"
    },
    
    # ========================================================================
    # BUILDING FACILITY TABLE
    # ========================================================================
    "fardap_silver_buildingfacility.content_type_value": {
        "lookup_table": "lu_pfbuildingfacilitiesprovidedtype",
        "description": "Type of building facility"
    },
    "fardap_silver_buildingfacility.content_notworkingdueto_value": {
        "lookup_table": "lu_pfbuildingfacilitiesnotworkingtype",
        "description": "Reason facility not working"
    },
    
    # ========================================================================
    # MANUAL SYSTEM TABLE
    # ========================================================================
    "fardap_silver_manualsystem.content_type_value": {
        "lookup_table": "lu_pfmanualsystemsprovidedtype",
        "description": "Type of manual system"
    },
    "fardap_silver_manualsystem.content_impact_value": {
        "lookup_table": "lu_pfmanualsystemsimpacttype",
        "description": "Impact of manual system"
    },
    
    # ========================================================================
    # SYSTEM TABLE (alarm/safety systems)
    # ========================================================================
    "fardap_silver_system.content_type_value": {
        "lookup_table": "lu_afsafetysystemspresenttype",
        "description": "Type of safety system"
    },
    "fardap_silver_system.content_operated_value": {
        "lookup_table": "lu_afsafetysystemsoperatedtype",
        "description": "Whether system operated"
    },
    "fardap_silver_system.content_location_value": {
        "lookup_table": "lu_afsafetysystemslocationtype",
        "description": "Location of system"
    },
    "fardap_silver_system.content_reasonforpooroutcome_value": {
        "lookup_table": "lu_afsafetysystemspooroutcometype",
        "description": "Reason for poor outcome"
    },
    "fardap_silver_system.content_impact_value": {
        "lookup_table": "lu_afsafetysystemsimpacttype",
        "description": "Impact of system"
    },
}

# Helper function to get lookup for a column
def get_lookup_table(silver_table, column_name):
    """Get the lookup table for a given silver table column."""
    key = f"{silver_table}.{column_name}"
    mapping = LOOKUP_MAPPINGS.get(key)
    if mapping:
        return mapping["lookup_table"]
    return None

# Print summary
if __name__ == "__main__":
    print(f"Total mappings defined: {len(LOOKUP_MAPPINGS)}")
    print("\nMappings by table:")
    
    from collections import defaultdict
    by_table = defaultdict(list)
    for key in LOOKUP_MAPPINGS:
        table = key.split(".")[0]
        by_table[table].append(key)
    
    for table, cols in sorted(by_table.items()):
        print(f"\n  {table}: {len(cols)} lookups")
