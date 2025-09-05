# TR2000 API Endpoints Documentation

This document lists all available endpoints in the TR2000 API Data Manager with their parameters and response field datatypes.

## Section 1: Operators and Plants

### 1.1 Get operators
- **Endpoint:** `operators`
- **Method:** GET
- **Parameters:** None
- **Response Fields:**
  - `OperatorID` → [Int32]
  - `OperatorName` → [String]

### 1.2 Get operator plants
- **Endpoint:** `operators/{operatorid}/plants`
- **Method:** GET
- **Parameters:**
  - `OPERATORID` (required) → Dropdown selection of operators
- **Response Fields:**
  - `OperatorID` → [Int32]
  - `OperatorName` → [String]
  - `PlantID` → [String]
  - `ShortDescription` → [String]
  - `Project` → [String]
  - `LongDescription` → [String]
  - `CommonLibPlantCode` → [String]
  - `InitialRevision` → [String]
  - `AreaID` → [Int32]
  - `Area` → [String]

### 1.3 Get plants
- **Endpoint:** `plants`
- **Method:** GET
- **Parameters:** None
- **Response Fields:**
  - `OperatorID` → [Int32]
  - `OperatorName` → [String]
  - `PlantID` → [String]
  - `ShortDescription` → [String]
  - `Project` → [String]
  - `LongDescription` → [String]
  - `CommonLibPlantCode` → [String]
  - `InitialRevision` → [String]
  - `AreaID` → [Int32]
  - `Area` → [String]

### 1.4 Get plant
- **Endpoint:** `plants/{plantid}`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
- **Response Fields:**
  - `OperatorID` → [Int32]
  - `OperatorName` → [String]
  - `PlantID` → [String]
  - `ShortDescription` → [String]
  - `Project` → [String]
  - `LongDescription` → [String]
  - `CommonLibPlantCode` → [String]
  - `InitialRevision` → [String]
  - `AreaID` → [Int32]
  - `Area` → [String]
  - `EnableEmbeddedNote` → [String]
  - `CategoryID` → [String]
  - `Category` → [String]
  - `DocumentSpaceLink` → [String]
  - `EnableCopyPCSFromPlant` → [String]
  - `OverLength` → [String]
  - `PCSQA` → [String]
  - `EDSMJ` → [String]
  - `CelsiusBar` → [String]
  - `WebInfoText` → [String]
  - `BoltTensionText` → [String]
  - `Visible` → [String]
  - `WindowsRemarkText` → [String]
  - `UserProtected` → [String]

## Section 2: Issues - Collection of datasheets

### 2.1 Get issue revisions
- **Endpoint:** `plants/{plantid}/issues`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
- **Response Fields:**
  - `IssueRevision` → [String]
  - `Status` → [String]
  - `RevDate` → [String]
  - `ProtectStatus` → [String]
  - `GeneralRevision` → [String]
  - `GeneralRevDate` → [String]
  - `PCSRevision` → [String]
  - `PCSRevDate` → [String]
  - `EDSRevision` → [String]
  - `EDSRevDate` → [String]
  - `VDSRevision` → [String]
  - `VDSRevDate` → [String]
  - `VSKRevision` → [String]
  - `VSKRevDate` → [String]
  - `MDSRevision` → [String]
  - `MDSRevDate` → [String]
  - `ESKRevision` → [String]
  - `ESKRevDate` → [String]
  - `SCRevision` → [String]
  - `SCRevDate` → [String]
  - `VSMRevision` → [String]
  - `VSMRevDate` → [String]
  - `UserName` → [String]
  - `UserEntryTime` → [String]
  - `UserProtected` → [String]

### 2.2 Get PCS references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/pcs`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `PCS` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `RevisionSuffix` → [String]
  - `RatingClass` → [String]
  - `MaterialGroup` → [String]
  - `HistoricalPCS` → [String]
  - `Delta` → [String]

### 2.3 Get SC references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/sc`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `SC` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.4 Get VSM references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/vsm`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `VSM` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.5 Get VDS references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/vds`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `VDS` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.6 Get EDS references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/eds`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `EDS` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.7 Get MDS references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/mds`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `MDS` → [String]
  - `Revision` → [String]
  - `Area` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.8 Get VSK references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/vsk`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `VSK` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.9 Get ESK references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/esk`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `ESK` → [String]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `OfficialRevision` → [String]
  - `Delta` → [String]

### 2.10 Get Pipe Element references
- **Endpoint:** `plants/{plantid}/issues/rev/{issuerev}/pipe-elements`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `ISSUEREV` (required) → Dropdown selection of issue revisions (depends on PLANTID)
- **Response Fields:**
  - `ElementGroup` → [String]
  - `DimensionStandard` → [String]
  - `ProductForm` → [String]
  - `MaterialGrade` → [String]
  - `MDS` → [String]
  - `MDSRevision` → [String]
  - `Area` → [String]
  - `ElementID` → [Int32]
  - `Revision` → [String]
  - `RevDate` → [String]
  - `Status` → [String]
  - `Delta` → [String]

## Section 3: PCS

### 3.1 Get PCS list
- **Endpoint:** `plants/{plantid}/pcs`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `NAMEFILTER` (optional) → String (query parameter)
  - `STATUSFILTER` (optional) → String (query parameter)
  - `NOTEID` (optional) → Int32 (query parameter)
  - `VDS` (optional) → String (query parameter)
  - `ELEMENTID` (optional) → Int32 (query parameter)
- **Response Fields:**
  - `PCS` → [String]
  - `Revision` → [String]
  - `Status` → [String]
  - `RevDate` → [String]
  - `RatingClass` → [String]
  - `TestPressure` → [String]
  - `MaterialGroup` → [String]
  - `DesignCode` → [String]
  - `LastUpdate` → [String]
  - `LastUpdateBy` → [String]
  - `Approver` → [String]
  - `Notepad` → [String]
  - `SpecialReqID` → [Int32]
  - `TubePCS` → [String]
  - `NewVDSSection` → [String]

### 3.2 Get header and properties
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `PCS` → [String]
  - `Revision` → [String]
  - `Status` → [String]
  - `RevDate` → [String]
  - `RatingClass` → [String]
  - `TestPressure` → [String]
  - `MaterialGroup` → [String]
  - `DesignCode` → [String]
  - `LastUpdate` → [String]
  - `LastUpdateBy` → [String]
  - `Approver` → [String]
  - `Notepad` → [String]
  - `SC` → [String]
  - `VSM` → [String]
  - `DesignCodeRevMark` → [String]
  - `CorrAllowance` → [Int32]
  - `CorrAllowanceRevMark` → [String]
  - `LongWeldEff` → [String]
  - `LongWeldEffRevMark` → [String]
  - `WallThkTol` → [String]
  - `WallThkTolRevMark` → [String]
  - `ServiceRemark` → [String]
  - `ServiceRemarkRevMark` → [String]
  - `DesignPress01` → [String]
  - `DesignPress02` → [String]
  - `DesignPress03` → [String]
  - `DesignPress04` → [String]
  - `DesignPress05` → [String]
  - `DesignPress06` → [String]
  - `DesignPress07` → [String]
  - `DesignPress08` → [String]
  - `DesignPress09` → [String]
  - `DesignPress10` → [String]
  - `DesignPress11` → [String]
  - `DesignPress12` → [String]
  - `DesignPressRevMark` → [String]
  - `DesignTemp01` → [String]
  - `DesignTemp02` → [String]
  - `DesignTemp03` → [String]
  - `DesignTemp04` → [String]
  - `DesignTemp05` → [String]
  - `DesignTemp06` → [String]
  - `DesignTemp07` → [String]
  - `DesignTemp08` → [String]
  - `DesignTemp09` → [String]
  - `DesignTemp10` → [String]
  - `DesignTemp11` → [String]
  - `DesignTemp12` → [String]
  - `DesignTempRevMark` → [String]
  - `NoteIDCorrAllowance` → [String]
  - `NoteIDServiceCode` → [String]
  - `NoteIDWallThkTol` → [String]
  - `NoteIDLongWeldEff` → [String]
  - `NoteIDGeneralPCS` → [String]
  - `NoteIDDesignCode` → [String]
  - `NoteIDPressTempTable` → [String]
  - `NoteIDPipeSizeWthTable` → [String]
  - `PressElementChange` → [String]
  - `TempElementChange` → [String]
  - `MaterialGroupID` → [Int32]
  - `SpecialReqID` → [Int32]
  - `SpecialReq` → [String]
  - `NewVDSSection` → [String]
  - `TubePCS` → [String]
  - `EDSMJMatrix` → [String]
  - `MJReductionFactor` → [Int32]

### 3.3 Get temperature and pressure
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}/temp-pressures`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `Temperature` → [String]
  - `Pressure` → [String]

### 3.4 Get pipe size
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-sizes`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `PCS` → [String]
  - `Revision` → [String]
  - `NomSize` → [String]
  - `OuterDiam` → [String]
  - `WallThickness` → [String]
  - `Schedule` → [String]
  - `UnderTolerance` → [String]
  - `CorrosionAllowance` → [String]
  - `WeldingFactor` → [String]
  - `DimElementChange` → [String]
  - `ScheduleInMatrix` → [String]

### 3.5 Get pipe element
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-elements`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `PCS` → [String]
  - `Revision` → [String]
  - `MaterialGroupID` → [Int32]
  - `ElementGroupNo` → [Int32]
  - `LineNo` → [Int32]
  - `Element` → [String]
  - `DimStandard` → [String]
  - `FromSize` → [String]
  - `ToSize` → [String]
  - `ProductForm` → [String]
  - `Material` → [String]
  - `MDS` → [String]
  - `EDS` → [String]
  - `EDSRevision` → [String]
  - `ESK` → [String]
  - `Revmark` → [String]
  - `Remark` → [String]
  - `PageBreak` → [String]
  - `ElementID` → [Int32]
  - `FreeText` → [String]
  - `NoteID` → [String]
  - `NewDeletedLine` → [String]
  - `InitialInfo` → [String]
  - `InitialRevmark` → [String]
  - `MDSVariant` → [String]
  - `MDSRevision` → [String]
  - `Area` → [String]

### 3.6 Get valve element
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}/valve-elements`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `ValveGroupNo` → [Int32]
  - `LineNo` → [Int32]
  - `ValveType` → [String]
  - `VDS` → [String]
  - `ValveDescription` → [String]
  - `FromSize` → [String]
  - `ToSize` → [String]
  - `Revmark` → [String]
  - `Remark` → [String]
  - `PageBreak` → [String]
  - `NoteID` → [String]
  - `PreviousVDS` → [String]
  - `NewDeletedLine` → [String]
  - `InitialInfo` → [String]
  - `InitialRevmark` → [String]
  - `SizeRange` → [String]
  - `Status` → [String]
  - `Revision` → [String]

### 3.7 Get embedded note
- **Endpoint:** `plants/{plantid}/pcs/{pcsname}/rev/{revision}/embedded-notes`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plants
  - `PCSNAME` (required) → Dropdown selection of PCS names (depends on PLANTID)
  - `REVISION` (required) → Dropdown selection of revisions (depends on PCSNAME)
- **Response Fields:**
  - `PCSName` → [String]
  - `Revision` → [String]
  - `TextSectionID` → [String]
  - `TextSectionDescription` → [String]
  - `PageBreak` → [String]
  - `HTMLCLOB` → [String]

## Section 4: VDS

### 4.1 Get VDS list
- **Endpoint:** `vds`
- **Method:** GET
- **Parameters:**
  - `NAMEFILTER` (optional) → String (query parameter)
  - `STATUSFILTER` (optional) → String (query parameter)
  - `BASEDONSUBSEGMENT` (optional) → String (query parameter)
  - `VDS` (optional) → String (query parameter)
  - `VALVETYPEID` (optional) → Int32 (query parameter)
  - `RATINGCLASSID` (optional) → Int32 (query parameter)
  - `MATERIALGROUPID` (optional) → Int32 (query parameter)
  - `ENDCONNECTIONID` (optional) → Int32 (query parameter)
  - `BOREID` (optional) → Int32 (query parameter)
  - `VDSSIZEID` (optional) → Int32 (query parameter)
  - `SPECIALREQID` (optional) → Int32 (query parameter)
  - `SUBSEGMENTREF` (optional) → Int32 (query parameter)
  - `TEXTBLOCKID` (optional) → Int32 (query parameter)
- **Response Fields:**
  - `VDS` → [String]
  - `Revision` → [String]
  - `Status` → [String]
  - `RevDate` → [String]
  - `LastUpdate` → [String]
  - `LastUpdateBy` → [String]
  - `Description` → [String]
  - `Notepad` → [String]
  - `SpecialReqID` → [Int32]
  - `ValveTypeID` → [Int32]
  - `RatingClassID` → [Int32]
  - `MaterialGroupID` → [Int32]
  - `EndConnectionID` → [Int32]
  - `BoreID` → [Int32]
  - `VDSSizeID` → [Int32]
  - `SizeRange` → [String]
  - `CustomName` → [String]
  - `SubsegmentList` → [String]

### 4.2 Get subsegments and properties
- **Endpoint:** `vds/{vdsname}/rev/{revision}`
- **Method:** GET
- **Parameters:**
  - `VDSNAME` (required) → Text input
  - `REVISION` (required) → Text input
- **Response Fields:**
  - `ValveTypeID` → [Int32]
  - `RatingClassID` → [Int32]
  - `MaterialTypeID` → [Int32]
  - `EndConnectionID` → [Int32]
  - `FullReducedBoreIndicator` → [String]
  - `BoreID` → [Int32]
  - `VDSSizeID` → [Int32]
  - `HousingDesignIndicator` → [String]
  - `HousingDesignID` → [Int32]
  - `SpecialReqID` → [Int32]
  - `MinOperatingTemperature` → [Int32]
  - `MaxOperatingTemperature` → [Int32]
  - `VDSDescription` → [String]
  - `Notepad` → [String]
  - `RevDate` → [String]
  - `LastUpdate` → [String]
  - `LastUpdateBy` → [String]
  - `SubsegmentID` → [Int32]
  - `SubsegmentName` → [String]
  - `Sequence` → [Int32]

## Section 5: BoltTension

### 5.1 Get Flange Type
- **Endpoint:** `BoltTension/getFlangeType/{plantid}/{pcs}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
  - `PCS` (required) → Dropdown selection of PCS (depends on PLANTID)
  - `FlangeSize` (required) → Dropdown selection (1-100) (query parameter)
- **Response Fields:**
  - `Display` → [String]
  - `FlangeTypeId` → [Int32]
  - `ComponentType` → [String]
  - `FlangeOrMechjoint` → [String]
  - `RatingClass` → [String]

### 5.2 Get Gasket Type
- **Endpoint:** `BoltTension/getGasketType/{plantid}/{pcs}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
  - `PCS` (required) → Dropdown selection of PCS (depends on PLANTID)
  - `FlangeTypeId` (required) → Int32 (query parameter)
  - `FlangeSize` (required) → Dropdown selection (1-100) (query parameter)
- **Response Fields:**
  - `GasketId` → [Int32]
  - `Display` → [String]

### 5.3 Get Bolt Material
- **Endpoint:** `BoltTension/getBoltMaterial/{plantid}/{pcs}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
  - `PCS` (required) → Dropdown selection of PCS (depends on PLANTID)
  - `FlangeTypeId` (required) → Int32 (query parameter)
  - `LubricantId` (required) → Int32 (query parameter)
- **Response Fields:**
  - `BoltMaterialId` → [Int32]
  - `Display` → [String]

### 5.4 Get Tension Forces
- **Endpoint:** `BoltTension/getTensionForces/{plantid}/{pcs}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
  - `PCS` (required) → Dropdown selection of PCS (depends on PLANTID)
  - `FlangeTypeId` (required) → Int32 (query parameter)
  - `GasketTypeId` (required) → Int32 (query parameter)
  - `BoltMaterialId` (required) → Int32 (query parameter)
  - `FlangeSize` (required) → Dropdown selection (1-100) (query parameter)
  - `ComponentType` (required) → Text input (query parameter)
  - `LubricantId` (required) → Int32 (query parameter)
- **Response Fields:**
  - `NoOfBolts` → [Int32]
  - `BoltDiameter` → [String]
  - `BoltDiameterDisplay` → [String]
  - `NutNomSize` → [String]
  - `kn` → [Int32]
  - `nm` → [Int32]

### 5.5 Get Tool
- **Endpoint:** `BoltTension/getTool/{plantid}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
  - `BoltDim` (required) → Text input (query parameter)
- **Response Fields:**
  - `ToolId` → [Int32]
  - `Display` → [String]
  - `PlantDefault` → [String]

### 5.6 Get Tool Pressure
- **Endpoint:** `BoltTension/getToolPressure/`
- **Method:** GET
- **Parameters:**
  - `ToolId` (required) → Int32 (query parameter)
  - `BoltForceKN` (required) → Int32 (query parameter)
  - `TorqueNM` (required) → Int32 (query parameter)
  - `FlangeOrMechjoint` (required) → Text input (query parameter)
- **Response Fields:**
  - `ToolPressureA` → [Int32]
  - `ToolPressureB` → [Int32]
  - `Unit` → [String]

### 5.7 Get Plant Info
- **Endpoint:** `BoltTension/getPlantInfo/{plantid}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
- **Response Fields:**
  - `PlantName` → [String]
  - `ToolSerie` → [String]
  - `Lubricant` → [String]

### 5.8 Get Lubricant
- **Endpoint:** `BoltTension/getLubricant/{plantid}/`
- **Method:** GET
- **Parameters:**
  - `PLANTID` (required) → Dropdown selection of plant codes
- **Response Fields:**
  - `LubricantId` → [Int32]
  - `Display` → [String]
  - `PlantDefault` → [Int32]

## Parameter Types Legend

- **Dropdown selection** → Data populated from other API endpoints
- **Text input** → Free text field
- **Int32** → Integer number input
- **String** → Text data
- **(query parameter)** → Sent as URL query parameter instead of path parameter
- **(depends on X)** → Dropdown data filtered based on selection in parameter X

## Notes

1. All endpoints use HTTPS base URL: `https://equinor.pipespec-api.presight.com/`
2. Parameters marked as "required" must be provided for the endpoint to function
3. Dropdown selections are populated by calling the respective source endpoints
4. Query parameters are appended to the URL (e.g., `?param=value`)
5. Path parameters are embedded in the URL path (e.g., `/plants/{plantid}`)
6. The VDS list endpoint returns over 44,000 items and may take 30+ seconds to load
7. Some reference endpoints (sections 2.2-2.10) are marked for future implementation