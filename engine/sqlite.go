package engine

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jamestjat/sqlce/format"

	_ "modernc.org/sqlite"
)

// ExportToSQLite loads all tables from the SDF database into an in-memory
// SQLite database and returns it. The caller must close the returned DB.
func ExportToSQLite(db *Database) (*sql.DB, error) {
	sqliteDB, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		return nil, fmt.Errorf("creating in-memory SQLite: %w", err)
	}

	for _, name := range db.Tables() {
		tbl, err := db.Table(name)
		if err != nil {
			continue
		}
		result, err := tbl.Scan()
		if err != nil {
			continue
		}
		cols := tbl.Columns()
		if len(cols) == 0 {
			continue
		}

		createSQL := BuildCreateTable(name, cols)
		if _, err := sqliteDB.Exec(createSQL); err != nil {
			continue
		}
		if len(result.Rows) == 0 {
			continue
		}

		placeholders := make([]string, len(cols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf(`INSERT INTO "%s" VALUES (%s)`, name, strings.Join(placeholders, ","))

		tx, err := sqliteDB.Begin()
		if err != nil {
			continue
		}
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			continue
		}
		var execErr error
		for _, row := range result.Rows {
			args := make([]any, len(cols))
			for i := range cols {
				if i < len(row) {
					args[i] = row[i]
				}
			}
			if _, err := stmt.Exec(args...); err != nil {
				execErr = err
			}
		}
		stmt.Close()
		if execErr != nil {
			tx.Rollback()
			continue
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
		}
	}

	return sqliteDB, nil
}

func BuildCreateTable(name string, cols []format.ColumnDef) string {
	var parts []string
	for _, col := range cols {
		sqlType := ceTypeToSQLite(col.TypeID)
		parts = append(parts, fmt.Sprintf(`"%s" %s`, col.Name, sqlType))
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`, name, strings.Join(parts, ", "))
}

func ceTypeToSQLite(typeID uint16) string {
	switch typeID {
	case format.TypeTinyInt, format.TypeSmallInt, format.TypeInt, format.TypeBigInt, format.TypeBit:
		return "INTEGER"
	case format.TypeFloat, format.TypeReal:
		return "REAL"
	case format.TypeMoney, format.TypeNumeric:
		return "NUMERIC"
	case format.TypeNVarchar, format.TypeNChar, format.TypeNText, format.TypeUniqueIdentifier:
		return "TEXT"
	case format.TypeDatetime:
		return "TEXT"
	case format.TypeImage, format.TypeBinary, format.TypeVarBinary, format.TypeRowVersion:
		return "BLOB"
	default:
		return "TEXT"
	}
}

// ExtractControlLayer exports all tables to in-memory SQLite and runs
// the 8 control-layer SQL queries against it.
func ExtractControlLayer(db *Database) (*ControlLayerResult, error) {
	sqliteDB, err := ExportToSQLite(db)
	if err != nil {
		return nil, fmt.Errorf("exporting to SQLite: %w", err)
	}
	defer sqliteDB.Close()

	result := &ControlLayerResult{}

	result.ControlMatrix, err = queryQ1SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q1: %w", err)
	}
	result.CVRoleConstraints, err = queryQ2SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q2: %w", err)
	}
	result.EconomicFunctions, err = queryQ3SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q3: %w", err)
	}
	result.VariableTransforms, err = queryQ4SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q4: %w", err)
	}
	result.ModelMetadata, err = queryQ5SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q5: %w", err)
	}
	result.ExecutionSequence, err = queryQ6SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q6: %w", err)
	}
	result.UserParameters, err = queryQ7SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q7: %w", err)
	}
	result.LoopDetails, err = queryQ8SQL(sqliteDB)
	if err != nil {
		return nil, fmt.Errorf("Q8: %w", err)
	}

	return result, nil
}

func queryQ1SQL(db *sql.DB) ([]ControlMatrixRow, error) {
	const q = `
SELECT
    iinfo.Name,
    rb.BlockType,
    blks.Name,
    blks.Description,
    blks.ModelHorizonInSeconds,
    blks.LargestSettlingTimeInSeconds,
    blks.SmallestSettlingTimeInSeconds,
    blc.Representation,
    blc.IntendedMV,
    blc.IntendedModelLoopType,
    blc.Status,
    lpv.Name,
    lsp.Name,
    lop.Name,
    lmode.Name,
    linit.Name,
    l.Ranking,
    l.DCSSystem,
    l.PIDAlgorithm,
    l.PIDForm,
    l.PIDEquation,
    l.PVTrack,
    sisor.IsDisturbance,
    sisoe.Name,
    sisoe.SisoElementType,
    inputvrol.RoleType,
    inputiinfo.Name,
    inputpv.EngineeringUnits,
    inputiinfo.Description,
    inputpv.NormalMove,
    inputpv.MeasurementType,
    outputvrol.RoleType,
    outputiinfo.Name,
    outputpv.EngineeringUnits,
    outputiinfo.Description,
    outputpv.NormalMove,
    outputpv.MeasurementType,
    pare.TransferFunction,
    pare.IsActive,
    pare.Delay,
    pare.Gain,
    pare.Tau1,
    pare.Tau2,
    pare.Beta,
    pare.UncertaintyZoneTau,
    pare.UncertaintyZoneBeta,
    pare.BetaXGain,
    pare.UnitMinY,
    pare.UnitMaxY,
    pare.SettlingTimeInSeconds,
    pare.SamplingTime,
    pare.SettlingTimeControl
FROM Relation AS rels
    INNER JOIN ItemInformation AS iinfo   ON rels.RelationIdentifier   = iinfo.Identifier
    INNER JOIN RelationBlocks  AS rb      ON rels.RelationIdentifier   = rb.RelationIdentifier
    INNER JOIN Blocks          AS blks    ON rb.RelationBlockIdentifier = blks.BlockIdentifier
    INNER JOIN ModelLayerBlocks AS mlb    ON rb.RelationBlockIdentifier = mlb.ModelBlockId
    INNER JOIN ModelLayers     AS ml      ON mlb.ModelLayerId = ml.ModelLayerIdentifier
                                             AND ml.IsControlLayer = 1
    INNER JOIN SisoRelation    AS sisor   ON rb.RelationBlockIdentifier = sisor.RelationBlockIdentifier
                                             AND sisor.IsLegal = 1
    INNER JOIN SisoElements    AS sisoe   ON sisor.SisoRelationIdentifier = sisoe.SisoRelationIdentifier
    INNER JOIN ParametricElements AS pare ON sisoe.SisoElementIdentifier = pare.ElementIdentifier
                                             AND pare.TransferFunction IS NOT NULL
    INNER JOIN ProcessVariables       AS inputpv    ON sisor.InputProcessVariableIdentifier  = inputpv.ProcessVariableIdentifier
    INNER JOIN ItemInformation        AS inputiinfo ON inputpv.ProcessVariableIdentifier     = inputiinfo.Identifier
    INNER JOIN ControllerVariableReference AS inputcvref ON inputiinfo.Identifier = inputcvref.ProcessVariableId
    INNER JOIN VariableRole           AS inputvrol  ON inputcvref.ControllerVariableReferenceIdentifier = inputvrol.VariableReferenceIdentifier
                                                       AND inputvrol.RoleType IN ('MV', 'DV', 'POV')
    INNER JOIN ProcessVariables       AS outputpv    ON sisor.OutputProcessVariableIdentifier = outputpv.ProcessVariableIdentifier
    INNER JOIN ItemInformation        AS outputiinfo ON outputpv.ProcessVariableIdentifier    = outputiinfo.Identifier
    INNER JOIN ControllerVariableReference AS outputcvref ON outputiinfo.Identifier = outputcvref.ProcessVariableId
    INNER JOIN VariableRole           AS outputvrol  ON outputcvref.ControllerVariableReferenceIdentifier = outputvrol.VariableReferenceIdentifier
                                                        AND outputvrol.RoleType IN ('CV', 'POV', 'DV')
    LEFT JOIN BlcModel          AS blc    ON blks.BlockIdentifier = blc.BlcModelBlockId
    LEFT JOIN Loop              AS l      ON blc.LoopIdentifier   = l.LoopIdentifier
    LEFT JOIN ItemInformation   AS lpv    ON l.PV   = lpv.Identifier
    LEFT JOIN ItemInformation   AS lsp    ON l.SP   = lsp.Identifier
    LEFT JOIN ItemInformation   AS lop    ON l.OP   = lop.Identifier
    LEFT JOIN ItemInformation   AS lmode  ON l.MODE = lmode.Identifier
    LEFT JOIN ItemInformation   AS linit  ON l.INIT = linit.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ControlMatrixRow
	for rows.Next() {
		var (
			appName, blockType, blockName, blockDesc sql.NullString
			blockHorizon, blockLTSS, blockSTSS       sql.NullInt32
			blcConf, blcMV, blcLoop, blcStatus        sql.NullString
			lpvN, lspN, lopN, lmodeN, linitN          sql.NullString
			loopRank, loopDCS, loopPIDA, loopPIDF, loopPIDE sql.NullString
			loopPVTrack                                sql.NullBool
			isDisturb                                  sql.NullBool
			sisoElemName, sisoElemType                 sql.NullString
			inRole, inName, inEU, inDesc               sql.NullString
			inNM                                       sql.NullFloat64
			inMT                                       sql.NullString
			outRole, outName, outEU, outDesc            sql.NullString
			outNM                                      sql.NullFloat64
			outMT                                      sql.NullString
			tf                                         sql.NullString
			elemActive                                 sql.NullBool
			delay, gain, tau1, tau2, beta               sql.NullFloat64
			uzTau, uzBeta, bxg, minY, maxY             sql.NullFloat64
			tss                                        sql.NullInt32
			sampTime, stCtrl                           sql.NullFloat64
		)
		err := rows.Scan(
			&appName, &blockType, &blockName, &blockDesc,
			&blockHorizon, &blockLTSS, &blockSTSS,
			&blcConf, &blcMV, &blcLoop, &blcStatus,
			&lpvN, &lspN, &lopN, &lmodeN, &linitN,
			&loopRank, &loopDCS, &loopPIDA, &loopPIDF, &loopPIDE,
			&loopPVTrack,
			&isDisturb, &sisoElemName, &sisoElemType,
			&inRole, &inName, &inEU, &inDesc, &inNM, &inMT,
			&outRole, &outName, &outEU, &outDesc, &outNM, &outMT,
			&tf, &elemActive,
			&delay, &gain, &tau1, &tau2, &beta, &uzTau, &uzBeta, &bxg, &minY, &maxY,
			&tss, &sampTime, &stCtrl,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning Q1 row: %w", err)
		}
		r := ControlMatrixRow{
			AppName: appName.String, BlockType: blockType.String,
			BlockName: blockName.String, BlockDescription: blockDesc.String,
			BlockModelHorizon: blockHorizon.Int32, BlockLargestTSS: blockLTSS.Int32, BlockSmallestTSS: blockSTSS.Int32,
			IsDisturbance: isDisturb.Bool, SisoElementName: sisoElemName.String, SisoElementType: sisoElemType.String,
			InputRoleType: inRole.String, InputName: inName.String, InputEU: inEU.String,
			InputDescription: inDesc.String, InputNormalMove: inNM.Float64, InputMeasurementType: inMT.String,
			OutputRoleType: outRole.String, OutputName: outName.String, OutputEU: outEU.String,
			OutputDescription: outDesc.String, OutputNormalMove: outNM.Float64, OutputMeasurementType: outMT.String,
			TransferFunction: tf.String, ElementIsActive: elemActive.Bool,
			Delay: delay.Float64, Gain: gain.Float64, Tau1: tau1.Float64, Tau2: tau2.Float64, Beta: beta.Float64,
			UncertaintyZoneTau: uzTau.Float64, UncertaintyZoneBeta: uzBeta.Float64, BetaXGain: bxg.Float64,
			UnitMinY: minY.Float64, UnitMaxY: maxY.Float64, TSS: tss.Int32,
			SamplingTime: sampTime.Float64, SettlingTimeControl: stCtrl.Float64,
		}
		r.BLCConfiguration = nullStr(blcConf)
		r.BLCIntendedMV = nullStr(blcMV)
		r.BLCLoopType = nullStr(blcLoop)
		r.BLCStatus = nullStr(blcStatus)
		r.PV = nullStr(lpvN)
		r.SP = nullStr(lspN)
		r.OP = nullStr(lopN)
		r.LoopMODE = nullStr(lmodeN)
		r.LoopINIT = nullStr(linitN)
		r.LoopRanking = nullStr(loopRank)
		r.LoopDCSSystem = nullStr(loopDCS)
		r.LoopPIDAlgorithm = nullStr(loopPIDA)
		r.LoopPIDForm = nullStr(loopPIDF)
		r.LoopPIDEquation = nullStr(loopPIDE)
		if loopPVTrack.Valid {
			r.LoopPVTrack = &loopPVTrack.Bool
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func nullStr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	return &ns.String
}

func queryQ2SQL(db *sql.DB) ([]CVRoleConstraintRow, error) {
	const q = `
SELECT
    ii.Name, ii.Description, vr.RoleType,
    cv.IsLoLimitSpec, cv.IsHiLimitSpec, cv.IsSetPointSpec,
    cv.MinTimeToLimit, cv.RampRateImbalanceLo, cv.RampRateImbalanceHi,
    cv.RampImbalanceMethod
FROM CVRole cv
    INNER JOIN VariableRole vr ON cv.CVRoleIdentifier = vr.RoleIdentifier
    INNER JOIN ControllerVariableReference cvref ON vr.VariableReferenceIdentifier = cvref.ControllerVariableReferenceIdentifier
    INNER JOIN ItemInformation ii ON cvref.ProcessVariableId = ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CVRoleConstraintRow
	for rows.Next() {
		var name, desc, role, rampMeth sql.NullString
		var loLim, hiLim, spSpec sql.NullBool
		var mtl, rrLo, rrHi sql.NullFloat64
		err := rows.Scan(
			&name, &desc, &role,
			&loLim, &hiLim, &spSpec,
			&mtl, &rrLo, &rrHi, &rampMeth,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, CVRoleConstraintRow{
			VariableName: name.String, VariableDescription: desc.String, RoleType: role.String,
			IsLoLimitSpec: loLim.Bool, IsHiLimitSpec: hiLim.Bool, IsSetPointSpec: spSpec.Bool,
			MinTimeToLimit: mtl.Float64, RampRateImbalanceLo: rrLo.Float64, RampRateImbalanceHi: rrHi.Float64,
			RampImbalanceMethod: rampMeth.String,
		})
	}
	return result, rows.Err()
}

func queryQ3SQL(db *sql.DB) ([]EconomicFunctionRow, error) {
	const q = `
SELECT ii.Name, ef.FormulaString, ef.IsFormulaValid
FROM EconomicFunction ef
    INNER JOIN ItemInformation ii ON ef.ControllerIdentifier = ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []EconomicFunctionRow
	for rows.Next() {
		var name, formula sql.NullString
		var valid sql.NullBool
		if err := rows.Scan(&name, &formula, &valid); err != nil {
			return nil, err
		}
		result = append(result, EconomicFunctionRow{
			ControllerName: name.String, ObjectiveFormula: formula.String, IsFormulaValid: valid.Bool,
		})
	}
	return result, rows.Err()
}

func queryQ4SQL(db *sql.DB) ([]VariableTransformRow, error) {
	const q = `
SELECT ii.Name, vt.Formula, vt.Min, vt.Max, vt.PointNumber
FROM VariableTransform vt
    INNER JOIN ItemInformation ii ON vt.VariableIdentifier = ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []VariableTransformRow
	for rows.Next() {
		var name, formula sql.NullString
		var mn, mx sql.NullFloat64
		var pn sql.NullInt32
		if err := rows.Scan(&name, &formula, &mn, &mx, &pn); err != nil {
			return nil, err
		}
		result = append(result, VariableTransformRow{
			VariableName: name.String, Formula: formula.String,
			Min: mn.Float64, Max: mx.Float64, PointNumber: pn.Int32,
		})
	}
	return result, rows.Err()
}

func queryQ5SQL(db *sql.DB) ([]ModelMetadataRow, error) {
	const q = `
SELECT ii.Name, ii.Description,
    m.ModelHorizonInSeconds, m.PlotIntervalInSeconds,
    m.LargestSettlingTimeInSeconds, m.SmallestSettlingTimeInSeconds
FROM Models m
    INNER JOIN ItemInformation ii ON m.ModelIdentifier = ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ModelMetadataRow
	for rows.Next() {
		var name, desc sql.NullString
		var mh, pi, lt, st sql.NullInt32
		if err := rows.Scan(&name, &desc, &mh, &pi, &lt, &st); err != nil {
			return nil, err
		}
		result = append(result, ModelMetadataRow{
			ModelName: name.String, ModelDescription: desc.String,
			ModelHorizonInSeconds: mh.Int32, PlotIntervalInSeconds: pi.Int32,
			LargestSettlingTimeInSeconds: lt.Int32, SmallestSettlingTimeInSeconds: st.Int32,
		})
	}
	return result, rows.Err()
}

func queryQ6SQL(db *sql.DB) ([]ExecutionSequenceRow, error) {
	const q = `
SELECT es.ExecutionSequenceIdentifier, es.IsDefault, es.ExecutionIntervalInMilliseconds
FROM ExecutionSequence es`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ExecutionSequenceRow
	for rows.Next() {
		var esID sql.NullString
		var isDef sql.NullBool
		var interval sql.NullInt32
		if err := rows.Scan(&esID, &isDef, &interval); err != nil {
			return nil, err
		}
		result = append(result, ExecutionSequenceRow{
			ExecutionSequenceIdentifier: esID.String, IsDefault: isDef.Bool,
			ExecutionIntervalInMilliseconds: interval.Int32,
		})
	}
	return result, rows.Err()
}

func queryQ7SQL(db *sql.DB) ([]UserParameterRow, error) {
	const q = `
SELECT ii.Name, ii.Description,
    up.EngineeringUnits, up.UserParameterType,
    up.IsOperatorEditable, up.IsLocal
FROM UserParameter up
    INNER JOIN ItemInformation ii ON up.UserParameterId = ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []UserParameterRow
	for rows.Next() {
		var name, desc, eu, upt sql.NullString
		var opEdit, local sql.NullBool
		if err := rows.Scan(&name, &desc, &eu, &upt, &opEdit, &local); err != nil {
			return nil, err
		}
		result = append(result, UserParameterRow{
			ParameterName: name.String, ParameterDescription: desc.String,
			EngineeringUnits: eu.String, UserParameterType: upt.String,
			IsOperatorEditable: opEdit.Bool, IsLocal: local.Bool,
		})
	}
	return result, rows.Err()
}

func queryQ8SQL(db *sql.DB) ([]LoopDetailRow, error) {
	const q = `
SELECT
    lii.Name, lii.Description,
    lpv.Name, lsp.Name, lop.Name, lmode.Name, linit.Name,
    l.Ranking, l.DCSSystem, l.PIDAlgorithm, l.PIDForm, l.PIDEquation,
    l.LoopStatus, l.PVTrack,
    slave_ii.Name
FROM Loop l
    INNER JOIN ItemInformation  AS lii   ON l.LoopIdentifier = lii.Identifier
    LEFT JOIN  ItemInformation  AS lpv   ON l.PV        = lpv.Identifier
    LEFT JOIN  ItemInformation  AS lsp   ON l.SP        = lsp.Identifier
    LEFT JOIN  ItemInformation  AS lop   ON l.OP        = lop.Identifier
    LEFT JOIN  ItemInformation  AS lmode ON l.MODE      = lmode.Identifier
    LEFT JOIN  ItemInformation  AS linit ON l.INIT      = linit.Identifier
    LEFT JOIN  Loop             AS sl    ON l.SlaveLoop  = sl.LoopIdentifier
    LEFT JOIN  ItemInformation  AS slave_ii ON sl.LoopIdentifier = slave_ii.Identifier`

	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []LoopDetailRow
	for rows.Next() {
		var loopName, loopDesc sql.NullString
		var lpvN, lspN, lopN, lmodeN, linitN sql.NullString
		var rank, dcs, pidA, pidF, pidE, lStatus sql.NullString
		var pvTrack sql.NullBool
		var slaveN sql.NullString
		if err := rows.Scan(
			&loopName, &loopDesc,
			&lpvN, &lspN, &lopN, &lmodeN, &linitN,
			&rank, &dcs, &pidA, &pidF, &pidE,
			&lStatus, &pvTrack, &slaveN,
		); err != nil {
			return nil, err
		}
		r := LoopDetailRow{
			LoopName: loopName.String, LoopDescription: loopDesc.String,
			PV: nullStr(lpvN), SP: nullStr(lspN), OP: nullStr(lopN),
			MODE: nullStr(lmodeN), INIT: nullStr(linitN),
			Ranking: nullStr(rank), DCSSystem: nullStr(dcs),
			PIDAlgorithm: nullStr(pidA), PIDForm: nullStr(pidF), PIDEquation: nullStr(pidE),
			LoopStatus: nullStr(lStatus), SlaveLoopName: nullStr(slaveN),
		}
		if pvTrack.Valid {
			r.PVTrack = &pvTrack.Bool
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
