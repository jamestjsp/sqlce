package engine

import (
	"fmt"
)

type tableData struct {
	colIdx map[string]int
	rows   [][]any
}

type guidIndex map[string][]int

func newTableData(result *ScanResult) *tableData {
	td := &tableData{
		colIdx: make(map[string]int, len(result.Columns)),
		rows:   result.Rows,
	}
	for i, c := range result.Columns {
		td.colIdx[c.Name] = i
	}
	return td
}

func (td *tableData) indexBy(col string) guidIndex {
	idx := make(guidIndex, len(td.rows))
	ci, ok := td.colIdx[col]
	if !ok {
		return idx
	}
	for i, row := range td.rows {
		if ci < len(row) {
			if g, ok := row[ci].(string); ok && g != "" {
				idx[g] = append(idx[g], i)
			}
		}
	}
	return idx
}

func (td *tableData) str(row int, col string) string {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return ""
	}
	if s, ok := td.rows[row][ci].(string); ok {
		return s
	}
	return fmt.Sprintf("%v", td.rows[row][ci])
}

func (td *tableData) strp(row int, col string) *string {
	s := td.str(row, col)
	if s == "" {
		return nil
	}
	return &s
}

func (td *tableData) i32(row int, col string) int32 {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return 0
	}
	switch v := td.rows[row][ci].(type) {
	case int32:
		return v
	case int16:
		return int32(v)
	case int64:
		return int32(v)
	}
	return 0
}

func (td *tableData) f64(row int, col string) float64 {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return 0
	}
	switch v := td.rows[row][ci].(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	}
	return 0
}

func (td *tableData) boolean(row int, col string) bool {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return false
	}
	if b, ok := td.rows[row][ci].(bool); ok {
		return b
	}
	return false
}

func (td *tableData) boolp(row int, col string) *bool {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return nil
	}
	if b, ok := td.rows[row][ci].(bool); ok {
		return &b
	}
	return nil
}

type tablePool struct {
	tables map[string]*tableData
}

func loadTablePool(db *Database, names []string) (*tablePool, error) {
	pool := &tablePool{tables: make(map[string]*tableData, len(names))}
	for _, name := range names {
		tbl, err := db.Table(name)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		result, err := tbl.Scan()
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", name, err)
		}
		pool.tables[name] = newTableData(result)
	}
	return pool, nil
}

func (p *tablePool) t(name string) *tableData { return p.tables[name] }

// ExtractControlLayer runs all 8 control-layer queries via in-memory JOINs.
func ExtractControlLayer(db *Database) (*ControlLayerResult, error) {
	tables := []string{
		"Relation", "ItemInformation", "RelationBlocks", "Blocks",
		"ModelLayerBlocks", "ModelLayers", "SisoRelation", "SisoElements",
		"ParametricElements", "ProcessVariables", "ControllerVariableReference",
		"VariableRole", "BlcModel", "Loop", "CVRole", "EconomicFunction",
		"VariableTransform", "Models", "ExecutionSequence", "UserParameter",
	}

	pool, err := loadTablePool(db, tables)
	if err != nil {
		return nil, err
	}

	result := &ControlLayerResult{}
	result.ControlMatrix = queryQ1(pool)
	result.CVRoleConstraints = queryQ2(pool)
	result.EconomicFunctions = queryQ3(pool)
	result.VariableTransforms = queryQ4(pool)
	result.ModelMetadata = queryQ5(pool)
	result.ExecutionSequence = queryQ6(pool)
	result.UserParameters = queryQ7(pool)
	result.LoopDetails = queryQ8(pool)
	return result, nil
}

func queryQ1(p *tablePool) []ControlMatrixRow {
	rel := p.t("Relation")
	ii := p.t("ItemInformation")
	rb := p.t("RelationBlocks")
	blks := p.t("Blocks")
	mlb := p.t("ModelLayerBlocks")
	ml := p.t("ModelLayers")
	sisor := p.t("SisoRelation")
	sisoe := p.t("SisoElements")
	pare := p.t("ParametricElements")
	pv := p.t("ProcessVariables")
	cvref := p.t("ControllerVariableReference")
	vrol := p.t("VariableRole")
	blcm := p.t("BlcModel")
	lp := p.t("Loop")

	iiIdx := ii.indexBy("Identifier")
	rbIdx := rb.indexBy("RelationIdentifier")
	blkIdx := blks.indexBy("BlockIdentifier")
	mlbIdx := mlb.indexBy("ModelBlockId")
	mlIdx := ml.indexBy("ModelLayerIdentifier")
	sisorIdx := sisor.indexBy("RelationBlockIdentifier")
	sisoeIdx := sisoe.indexBy("SisoRelationIdentifier")
	pareIdx := pare.indexBy("ElementIdentifier")
	pvIdx := pv.indexBy("ProcessVariableIdentifier")
	cvrefIdx := cvref.indexBy("ProcessVariableId")
	vrolIdx := vrol.indexBy("VariableReferenceIdentifier")
	blcIdx := blcm.indexBy("BlcModelBlockId")
	lpIdx := lp.indexBy("LoopIdentifier")

	var rows []ControlMatrixRow

	for ri := range rel.rows {
		relID := rel.str(ri, "RelationIdentifier")
		appRows := iiIdx[relID]
		if len(appRows) == 0 {
			continue
		}
		appName := ii.str(appRows[0], "Name")

		for _, rbi := range rbIdx[relID] {
			rbID := rb.str(rbi, "RelationBlockIdentifier")
			blockType := rb.str(rbi, "BlockType")

			blkRows := blkIdx[rbID]
			if len(blkRows) == 0 {
				continue
			}
			bi := blkRows[0]

			if len(mlbIdx[rbID]) == 0 {
				continue
			}
			foundControlLayer := false
			for _, mi := range mlbIdx[rbID] {
				mlID := mlb.str(mi, "ModelLayerId")
				for _, mli := range mlIdx[mlID] {
					if bitTrue(ml, mli, "IsControlLayer") {
						foundControlLayer = true
					}
				}
			}
			if !foundControlLayer {
				continue
			}

			for _, si := range sisorIdx[rbID] {
				if !bitTrue(sisor, si, "IsLegal") {
					continue
				}

				sisorID := sisor.str(si, "SisoRelationIdentifier")
				for _, sei := range sisoeIdx[sisorID] {
					seID := sisoe.str(sei, "SisoElementIdentifier")
					for _, pei := range pareIdx[seID] {
						tf := pare.str(pei, "TransferFunction")
						if tf == "" {
							continue
						}

						inputPVID := sisor.str(si, "InputProcessVariableIdentifier")
						outputPVID := sisor.str(si, "OutputProcessVariableIdentifier")

						inputVar := resolveVarChain(inputPVID, pvIdx, pv, iiIdx, ii, cvrefIdx, cvref, vrolIdx, vrol, []string{"MV", "DV", "POV"})
						outputVar := resolveVarChain(outputPVID, pvIdx, pv, iiIdx, ii, cvrefIdx, cvref, vrolIdx, vrol, []string{"CV", "POV", "DV"})

						if inputVar == nil || outputVar == nil {
							continue
						}

						row := ControlMatrixRow{
							AppName:          appName,
							BlockType:        blockType,
							BlockName:        blks.str(bi, "Name"),
							BlockDescription: blks.str(bi, "Description"),
							BlockModelHorizon: blks.i32(bi, "ModelHorizonInSeconds"),
							BlockLargestTSS:   blks.i32(bi, "LargestSettlingTimeInSeconds"),
							BlockSmallestTSS:  blks.i32(bi, "SmallestSettlingTimeInSeconds"),

							IsDisturbance:   sisor.boolean(si, "IsDisturbance"),
							SisoElementName: sisoe.str(sei, "Name"),
							SisoElementType: sisoe.str(sei, "SisoElementType"),

							InputRoleType:        inputVar.roleType,
							InputName:            inputVar.name,
							InputEU:              inputVar.eu,
							InputDescription:     inputVar.description,
							InputNormalMove:      inputVar.normalMove,
							InputMeasurementType: inputVar.measurementType,

							OutputRoleType:        outputVar.roleType,
							OutputName:            outputVar.name,
							OutputEU:              outputVar.eu,
							OutputDescription:     outputVar.description,
							OutputNormalMove:      outputVar.normalMove,
							OutputMeasurementType: outputVar.measurementType,

							TransferFunction:    tf,
							ElementIsActive:     pare.boolean(pei, "IsActive"),
							Delay:               pare.f64(pei, "Delay"),
							Gain:                pare.f64(pei, "Gain"),
							Tau1:                pare.f64(pei, "Tau1"),
							Tau2:                pare.f64(pei, "Tau2"),
							Beta:                pare.f64(pei, "Beta"),
							UncertaintyZoneTau:  pare.f64(pei, "UncertaintyZoneTau"),
							UncertaintyZoneBeta: pare.f64(pei, "UncertaintyZoneBeta"),
							BetaXGain:           pare.f64(pei, "BetaXGain"),
							UnitMinY:            pare.f64(pei, "UnitMinY"),
							UnitMaxY:            pare.f64(pei, "UnitMaxY"),
							TSS:                 pare.i32(pei, "SettlingTimeInSeconds"),
							SamplingTime:        pare.f64(pei, "SamplingTime"),
							SettlingTimeControl: pare.f64(pei, "SettlingTimeControl"),
						}

						// LEFT JOINs: BlcModel → Loop → ItemInformation
						for _, blci := range blcIdx[rbID] {
							row.BLCConfiguration = blcm.strp(blci, "Representation")
							row.BLCIntendedMV = blcm.strp(blci, "IntendedMV")
							row.BLCLoopType = blcm.strp(blci, "IntendedModelLoopType")
							row.BLCStatus = blcm.strp(blci, "Status")

							loopID := blcm.str(blci, "LoopIdentifier")
							for _, li := range lpIdx[loopID] {
								row.LoopRanking = lp.strp(li, "Ranking")
								row.LoopDCSSystem = lp.strp(li, "DCSSystem")
								row.LoopPIDAlgorithm = lp.strp(li, "PIDAlgorithm")
								row.LoopPIDForm = lp.strp(li, "PIDForm")
								row.LoopPIDEquation = lp.strp(li, "PIDEquation")
								row.LoopPVTrack = lp.boolp(li, "PVTrack")

								pvII := iiIdx[lp.str(li, "PV")]
								if len(pvII) > 0 {
									row.PV = ii.strp(pvII[0], "Name")
								}
								spII := iiIdx[lp.str(li, "SP")]
								if len(spII) > 0 {
									row.SP = ii.strp(spII[0], "Name")
								}
								opII := iiIdx[lp.str(li, "OP")]
								if len(opII) > 0 {
									row.OP = ii.strp(opII[0], "Name")
								}
								modeII := iiIdx[lp.str(li, "MODE")]
								if len(modeII) > 0 {
									row.LoopMODE = ii.strp(modeII[0], "Name")
								}
								initII := iiIdx[lp.str(li, "INIT")]
								if len(initII) > 0 {
									row.LoopINIT = ii.strp(initII[0], "Name")
								}
							}
						}

						rows = append(rows, row)
					}
				}
			}
		}
	}
	return rows
}

type varChainResult struct {
	roleType        string
	name            string
	eu              string
	description     string
	normalMove      float64
	measurementType string
}

func resolveVarChain(pvID string, pvIdx guidIndex, pv *tableData, iiIdx guidIndex, ii *tableData,
	cvrefIdx guidIndex, cvref *tableData, vrolIdx guidIndex, vrol *tableData, roleTypes []string) *varChainResult {

	pvRows := pvIdx[pvID]
	if len(pvRows) == 0 {
		return nil
	}
	pi := pvRows[0]

	iiRows := iiIdx[pvID]
	if len(iiRows) == 0 {
		return nil
	}
	iir := iiRows[0]

	iiID := ii.str(iir, "Identifier")
	cvrefRows := cvrefIdx[iiID]
	if len(cvrefRows) == 0 {
		return nil
	}

	roleSet := make(map[string]bool, len(roleTypes))
	for _, rt := range roleTypes {
		roleSet[rt] = true
	}

	for _, cri := range cvrefRows {
		crID := cvref.str(cri, "ControllerVariableReferenceIdentifier")
		for _, vri := range vrolIdx[crID] {
			rt := vrol.str(vri, "RoleType")
			if roleSet[rt] {
				return &varChainResult{
					roleType:        rt,
					name:            ii.str(iir, "Name"),
					eu:              pv.str(pi, "EngineeringUnits"),
					description:     ii.str(iir, "Description"),
					normalMove:      pv.f64(pi, "NormalMove"),
					measurementType: pv.str(pi, "MeasurementType"),
				}
			}
		}
	}
	return nil
}

func queryQ2(p *tablePool) []CVRoleConstraintRow {
	cv := p.t("CVRole")
	vr := p.t("VariableRole")
	cvref := p.t("ControllerVariableReference")
	ii := p.t("ItemInformation")

	vrIdx := vr.indexBy("RoleIdentifier")
	cvrefIdx := cvref.indexBy("ControllerVariableReferenceIdentifier")
	iiIdx := ii.indexBy("Identifier")

	var rows []CVRoleConstraintRow
	for ri := range cv.rows {
		cvID := cv.str(ri, "CVRoleIdentifier")
		for _, vri := range vrIdx[cvID] {
			vrID := vr.str(vri, "VariableReferenceIdentifier")
			for _, cri := range cvrefIdx[vrID] {
				pvID := cvref.str(cri, "ProcessVariableId")
				for _, iii := range iiIdx[pvID] {
					rows = append(rows, CVRoleConstraintRow{
						VariableName:        ii.str(iii, "Name"),
						VariableDescription: ii.str(iii, "Description"),
						RoleType:            vr.str(vri, "RoleType"),
						IsLoLimitSpec:       cv.boolean(ri, "IsLoLimitSpec"),
						IsHiLimitSpec:       cv.boolean(ri, "IsHiLimitSpec"),
						IsSetPointSpec:      cv.boolean(ri, "IsSetPointSpec"),
						MinTimeToLimit:      cv.f64(ri, "MinTimeToLimit"),
						RampRateImbalanceLo: cv.f64(ri, "RampRateImbalanceLo"),
						RampRateImbalanceHi: cv.f64(ri, "RampRateImbalanceHi"),
						RampImbalanceMethod: cv.str(ri, "RampImbalanceMethod"),
					})
				}
			}
		}
	}
	return rows
}

func queryQ3(p *tablePool) []EconomicFunctionRow {
	ef := p.t("EconomicFunction")
	ii := p.t("ItemInformation")
	iiIdx := ii.indexBy("Identifier")

	var rows []EconomicFunctionRow
	for ri := range ef.rows {
		ctrlID := ef.str(ri, "ControllerIdentifier")
		for _, iii := range iiIdx[ctrlID] {
			rows = append(rows, EconomicFunctionRow{
				ControllerName:   ii.str(iii, "Name"),
				ObjectiveFormula: ef.str(ri, "FormulaString"),
				IsFormulaValid:   ef.boolean(ri, "IsFormulaValid"),
			})
		}
	}
	return rows
}

func queryQ4(p *tablePool) []VariableTransformRow {
	vt := p.t("VariableTransform")
	ii := p.t("ItemInformation")
	iiIdx := ii.indexBy("Identifier")

	var rows []VariableTransformRow
	for ri := range vt.rows {
		varID := vt.str(ri, "VariableIdentifier")
		for _, iii := range iiIdx[varID] {
			rows = append(rows, VariableTransformRow{
				VariableName: ii.str(iii, "Name"),
				Formula:      vt.str(ri, "Formula"),
				Min:          vt.f64(ri, "Min"),
				Max:          vt.f64(ri, "Max"),
				PointNumber:  vt.i32(ri, "PointNumber"),
			})
		}
	}
	return rows
}

func queryQ5(p *tablePool) []ModelMetadataRow {
	m := p.t("Models")
	ii := p.t("ItemInformation")
	iiIdx := ii.indexBy("Identifier")

	var rows []ModelMetadataRow
	for ri := range m.rows {
		mID := m.str(ri, "ModelIdentifier")
		for _, iii := range iiIdx[mID] {
			rows = append(rows, ModelMetadataRow{
				ModelName:                    ii.str(iii, "Name"),
				ModelDescription:             ii.str(iii, "Description"),
				ModelHorizonInSeconds:        m.i32(ri, "ModelHorizonInSeconds"),
				PlotIntervalInSeconds:        m.i32(ri, "PlotIntervalInSeconds"),
				LargestSettlingTimeInSeconds: m.i32(ri, "LargestSettlingTimeInSeconds"),
				SmallestSettlingTimeInSeconds: m.i32(ri, "SmallestSettlingTimeInSeconds"),
			})
		}
	}
	return rows
}

func queryQ6(p *tablePool) []ExecutionSequenceRow {
	es := p.t("ExecutionSequence")
	var rows []ExecutionSequenceRow
	for ri := range es.rows {
		rows = append(rows, ExecutionSequenceRow{
			ExecutionSequenceIdentifier:     es.str(ri, "ExecutionSequenceIdentifier"),
			IsDefault:                       es.boolean(ri, "IsDefault"),
			ExecutionIntervalInMilliseconds: es.i32(ri, "ExecutionIntervalInMilliseconds"),
		})
	}
	return rows
}

func queryQ7(p *tablePool) []UserParameterRow {
	up := p.t("UserParameter")
	ii := p.t("ItemInformation")
	iiIdx := ii.indexBy("Identifier")

	var rows []UserParameterRow
	for ri := range up.rows {
		upID := up.str(ri, "UserParameterId")
		for _, iii := range iiIdx[upID] {
			rows = append(rows, UserParameterRow{
				ParameterName:        ii.str(iii, "Name"),
				ParameterDescription: ii.str(iii, "Description"),
				EngineeringUnits:     up.str(ri, "EngineeringUnits"),
				UserParameterType:    up.str(ri, "UserParameterType"),
				IsOperatorEditable:   up.boolean(ri, "IsOperatorEditable"),
				IsLocal:              up.boolean(ri, "IsLocal"),
			})
		}
	}
	return rows
}

func queryQ8(p *tablePool) []LoopDetailRow {
	lp := p.t("Loop")
	ii := p.t("ItemInformation")
	iiIdx := ii.indexBy("Identifier")
	lpIdx := lp.indexBy("LoopIdentifier")

	var rows []LoopDetailRow
	for ri := range lp.rows {
		loopID := lp.str(ri, "LoopIdentifier")
		iiRows := iiIdx[loopID]
		if len(iiRows) == 0 {
			continue
		}

		row := LoopDetailRow{
			LoopName:        ii.str(iiRows[0], "Name"),
			LoopDescription: ii.str(iiRows[0], "Description"),
			PV:              iiName(ii, iiIdx, lp.str(ri, "PV")),
			SP:              iiName(ii, iiIdx, lp.str(ri, "SP")),
			OP:              iiName(ii, iiIdx, lp.str(ri, "OP")),
			MODE:            iiName(ii, iiIdx, lp.str(ri, "MODE")),
			INIT:            iiName(ii, iiIdx, lp.str(ri, "INIT")),
			Ranking:         lp.strp(ri, "Ranking"),
			DCSSystem:       lp.strp(ri, "DCSSystem"),
			PIDAlgorithm:    lp.strp(ri, "PIDAlgorithm"),
			PIDForm:         lp.strp(ri, "PIDForm"),
			PIDEquation:     lp.strp(ri, "PIDEquation"),
			LoopStatus:      lp.strp(ri, "LoopStatus"),
			PVTrack:         lp.boolp(ri, "PVTrack"),
		}

		slaveID := lp.str(ri, "SlaveLoop")
		if slaveID != "" {
			for _, sli := range lpIdx[slaveID] {
				slaveLoopID := lp.str(sli, "LoopIdentifier")
				siiRows := iiIdx[slaveLoopID]
				if len(siiRows) > 0 {
					row.SlaveLoopName = ii.strp(siiRows[0], "Name")
				}
			}
		}

		rows = append(rows, row)
	}
	return rows
}

// bitTrue checks a bit column value. Since bit columns have FixedSize=0
// (values stored in null bitmap, not yet extracted), nil values default to true.
func bitTrue(td *tableData, row int, col string) bool {
	ci, ok := td.colIdx[col]
	if !ok || ci >= len(td.rows[row]) || td.rows[row][ci] == nil {
		return true // default: bit values not yet parsed from bitmap
	}
	if b, ok := td.rows[row][ci].(bool); ok {
		return b
	}
	return true
}

func iiName(ii *tableData, iiIdx guidIndex, id string) *string {
	if id == "" {
		return nil
	}
	iiRows := iiIdx[id]
	if len(iiRows) == 0 {
		return nil
	}
	return ii.strp(iiRows[0], "Name")
}
