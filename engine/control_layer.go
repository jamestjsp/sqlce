package engine

import (
	"fmt"
)

type row = map[string]interface{}

func loadTable(db *Database, name string) ([]row, error) {
	tbl, err := db.Table(name)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", name, err)
	}
	result, err := tbl.Scan()
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", name, err)
	}
	cols := tbl.Columns()
	rows := make([]row, 0, len(result.Rows))
	for _, r := range result.Rows {
		m := make(row, len(cols))
		for i, c := range cols {
			if i < len(r) {
				m[c.Name] = r[i]
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

func indexBy(rows []row, col string) map[string][]row {
	idx := make(map[string][]row)
	for _, r := range rows {
		if k := str(r[col]); k != "" {
			idx[k] = append(idx[k], r)
		}
	}
	return idx
}

func str(v interface{}) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprintf("%v", s)
	}
}

func strPtr(v interface{}) *string {
	if v == nil {
		return nil
	}
	s := str(v)
	return &s
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case int32:
		return b != 0
	case int16:
		return b != 0
	case uint8:
		return b != 0
	default:
		return false
	}
}

func boolPtr(v interface{}) *bool {
	if v == nil {
		return nil
	}
	b := toBool(v)
	return &b
}

func toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case int16:
		return float64(n)
	default:
		return 0
	}
}

func toInt32(v interface{}) int32 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case int16:
		return int32(n)
	case float64:
		return int32(n)
	default:
		return 0
	}
}

func ExtractControlLayer(db *Database) (*ControlLayerResult, error) {
	result := &ControlLayerResult{}
	var err error

	result.ControlMatrix, err = queryQ1(db)
	if err != nil {
		return nil, fmt.Errorf("Q1: %w", err)
	}
	result.CVRoleConstraints, err = queryQ2(db)
	if err != nil {
		return nil, fmt.Errorf("Q2: %w", err)
	}
	result.EconomicFunctions, err = queryQ3(db)
	if err != nil {
		return nil, fmt.Errorf("Q3: %w", err)
	}
	result.VariableTransforms, err = queryQ4(db)
	if err != nil {
		return nil, fmt.Errorf("Q4: %w", err)
	}
	result.ModelMetadata, err = queryQ5(db)
	if err != nil {
		return nil, fmt.Errorf("Q5: %w", err)
	}
	result.ExecutionSequence, err = queryQ6(db)
	if err != nil {
		return nil, fmt.Errorf("Q6: %w", err)
	}
	result.UserParameters, err = queryQ7(db)
	if err != nil {
		return nil, fmt.Errorf("Q7: %w", err)
	}
	result.LoopDetails, err = queryQ8(db)
	if err != nil {
		return nil, fmt.Errorf("Q8: %w", err)
	}

	return result, nil
}

func queryQ1(db *Database) ([]ControlMatrixRow, error) {
	relation, err := loadTable(db, "Relation")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}
	rb, err := loadTable(db, "RelationBlocks")
	if err != nil {
		return nil, err
	}
	blocks, err := loadTable(db, "Blocks")
	if err != nil {
		return nil, err
	}
	mlb, err := loadTable(db, "ModelLayerBlocks")
	if err != nil {
		return nil, err
	}
	ml, err := loadTable(db, "ModelLayers")
	if err != nil {
		return nil, err
	}
	sisoRel, err := loadTable(db, "SisoRelation")
	if err != nil {
		return nil, err
	}
	sisoElem, err := loadTable(db, "SisoElements")
	if err != nil {
		return nil, err
	}
	paramElem, err := loadTable(db, "ParametricElements")
	if err != nil {
		return nil, err
	}
	procVars, err := loadTable(db, "ProcessVariables")
	if err != nil {
		return nil, err
	}
	cvRef, err := loadTable(db, "ControllerVariableReference")
	if err != nil {
		return nil, err
	}
	varRole, err := loadTable(db, "VariableRole")
	if err != nil {
		return nil, err
	}
	blcModel, err := loadTable(db, "BlcModel")
	if err != nil {
		return nil, err
	}
	loopTbl, err := loadTable(db, "Loop")
	if err != nil {
		return nil, err
	}

	iinfoIdx := indexBy(iinfo, "Identifier")
	rbIdx := indexBy(rb, "RelationIdentifier")
	blocksIdx := indexBy(blocks, "BlockIdentifier")
	mlbIdx := indexBy(mlb, "ModelBlockId")

	controlMLIds := make(map[string]bool)
	for _, r := range ml {
		if toBool(r["IsControlLayer"]) {
			controlMLIds[str(r["ModelLayerIdentifier"])] = true
		}
	}

	sisoRelIdx := indexBy(sisoRel, "RelationBlockIdentifier")
	sisoElemIdx := indexBy(sisoElem, "SisoRelationIdentifier")
	paramElemIdx := indexBy(paramElem, "ElementIdentifier")
	procVarsIdx := indexBy(procVars, "ProcessVariableIdentifier")
	cvRefIdx := indexBy(cvRef, "ProcessVariableId")
	varRoleIdx := indexBy(varRole, "VariableReferenceIdentifier")
	blcIdx := indexBy(blcModel, "BlcModelBlockId")
	loopIdx := indexBy(loopTbl, "LoopIdentifier")

	var results []ControlMatrixRow

	for _, rel := range relation {
		relId := str(rel["RelationIdentifier"])
		if relId == "" {
			continue
		}

		appInfos := iinfoIdx[relId]
		if len(appInfos) == 0 {
			continue
		}
		appName := str(appInfos[0]["Name"])

		for _, rbRow := range rbIdx[relId] {
			blockId := str(rbRow["RelationBlockIdentifier"])
			if blockId == "" {
				continue
			}

			blockRows := blocksIdx[blockId]
			if len(blockRows) == 0 {
				continue
			}
			blk := blockRows[0]

			hasControlLayer := false
			for _, m := range mlbIdx[blockId] {
				if controlMLIds[str(m["ModelLayerId"])] {
					hasControlLayer = true
					break
				}
			}
			if !hasControlLayer {
				continue
			}

			for _, sr := range sisoRelIdx[blockId] {
				if !toBool(sr["IsLegal"]) {
					continue
				}

				sisoRelId := str(sr["SisoRelationIdentifier"])
				inputPVId := str(sr["InputProcessVariableIdentifier"])
				outputPVId := str(sr["OutputProcessVariableIdentifier"])

				for _, se := range sisoElemIdx[sisoRelId] {
					elemId := str(se["SisoElementIdentifier"])

					for _, pe := range paramElemIdx[elemId] {
						if pe["TransferFunction"] == nil {
							continue
						}

						inputPVs := procVarsIdx[inputPVId]
						if len(inputPVs) == 0 {
							continue
						}
						inputPV := inputPVs[0]

						inputIInfos := iinfoIdx[inputPVId]
						if len(inputIInfos) == 0 {
							continue
						}
						inputIInfo := inputIInfos[0]

						inputCVRefs := cvRefIdx[str(inputIInfo["Identifier"])]
						if len(inputCVRefs) == 0 {
							continue
						}

						outputPVs := procVarsIdx[outputPVId]
						if len(outputPVs) == 0 {
							continue
						}
						outputPV := outputPVs[0]

						outputIInfos := iinfoIdx[outputPVId]
						if len(outputIInfos) == 0 {
							continue
						}
						outputIInfo := outputIInfos[0]

						outputCVRefs := cvRefIdx[str(outputIInfo["Identifier"])]
						if len(outputCVRefs) == 0 {
							continue
						}

						for _, inputCVRef := range inputCVRefs {
							inputCVRefId := str(inputCVRef["ControllerVariableReferenceIdentifier"])
							for _, inputVR := range varRoleIdx[inputCVRefId] {
								inputRole := str(inputVR["RoleType"])
								if inputRole != "MV" && inputRole != "DV" && inputRole != "POV" {
									continue
								}

								for _, outputCVRef := range outputCVRefs {
									outputCVRefId := str(outputCVRef["ControllerVariableReferenceIdentifier"])
									for _, outputVR := range varRoleIdx[outputCVRefId] {
										outputRole := str(outputVR["RoleType"])
										if outputRole != "CV" && outputRole != "POV" && outputRole != "DV" {
											continue
										}

										r := ControlMatrixRow{
											AppName:          appName,
											BlockType:        str(rbRow["BlockType"]),
											BlockName:        str(blk["Name"]),
											BlockDescription: str(blk["Description"]),
											BlockModelHorizon: toInt32(blk["ModelHorizonInSeconds"]),
											BlockLargestTSS:   toInt32(blk["LargestSettlingTimeInSeconds"]),
											BlockSmallestTSS:  toInt32(blk["SmallestSettlingTimeInSeconds"]),

											IsDisturbance:   toBool(sr["IsDisturbance"]),
											SisoElementName: str(se["Name"]),
											SisoElementType: str(se["SisoElementType"]),

											InputRoleType:        inputRole,
											InputName:            str(inputIInfo["Name"]),
											InputEU:              str(inputPV["EngineeringUnits"]),
											InputDescription:     str(inputIInfo["Description"]),
											InputNormalMove:      toFloat64(inputPV["NormalMove"]),
											InputMeasurementType: str(inputPV["MeasurementType"]),

											OutputRoleType:        outputRole,
											OutputName:            str(outputIInfo["Name"]),
											OutputEU:              str(outputPV["EngineeringUnits"]),
											OutputDescription:     str(outputIInfo["Description"]),
											OutputNormalMove:      toFloat64(outputPV["NormalMove"]),
											OutputMeasurementType: str(outputPV["MeasurementType"]),

											TransferFunction:    str(pe["TransferFunction"]),
											ElementIsActive:     toBool(pe["IsActive"]),
											Delay:               toFloat64(pe["Delay"]),
											Gain:                toFloat64(pe["Gain"]),
											Tau1:                toFloat64(pe["Tau1"]),
											Tau2:                toFloat64(pe["Tau2"]),
											Beta:                toFloat64(pe["Beta"]),
											UncertaintyZoneTau:  toFloat64(pe["UncertaintyZoneTau"]),
											UncertaintyZoneBeta: toFloat64(pe["UncertaintyZoneBeta"]),
											BetaXGain:           toFloat64(pe["BetaXGain"]),
											UnitMinY:            toFloat64(pe["UnitMinY"]),
											UnitMaxY:            toFloat64(pe["UnitMaxY"]),
											TSS:                 toInt32(pe["SettlingTimeInSeconds"]),
											SamplingTime:        toFloat64(pe["SamplingTime"]),
											SettlingTimeControl: toFloat64(pe["SettlingTimeControl"]),
										}

										blcRows := blcIdx[blockId]
										if len(blcRows) > 0 {
											blc := blcRows[0]
											r.BLCConfiguration = strPtr(blc["Representation"])
											r.BLCIntendedMV = strPtr(blc["IntendedMV"])
											r.BLCLoopType = strPtr(blc["IntendedModelLoopType"])
											r.BLCStatus = strPtr(blc["Status"])

											loopId := str(blc["LoopIdentifier"])
											if loopId != "" {
												loops := loopIdx[loopId]
												if len(loops) > 0 {
													lp := loops[0]
													r.LoopRanking = strPtr(lp["Ranking"])
													r.LoopDCSSystem = strPtr(lp["DCSSystem"])
													r.LoopPIDAlgorithm = strPtr(lp["PIDAlgorithm"])
													r.LoopPIDForm = strPtr(lp["PIDForm"])
													r.LoopPIDEquation = strPtr(lp["PIDEquation"])
													r.LoopPVTrack = boolPtr(lp["PVTrack"])

													pvInfos := iinfoIdx[str(lp["PV"])]
													if len(pvInfos) > 0 {
														r.PV = strPtr(pvInfos[0]["Name"])
													}
													spInfos := iinfoIdx[str(lp["SP"])]
													if len(spInfos) > 0 {
														r.SP = strPtr(spInfos[0]["Name"])
													}
													opInfos := iinfoIdx[str(lp["OP"])]
													if len(opInfos) > 0 {
														r.OP = strPtr(opInfos[0]["Name"])
													}
													modeInfos := iinfoIdx[str(lp["MODE"])]
													if len(modeInfos) > 0 {
														r.LoopMODE = strPtr(modeInfos[0]["Name"])
													}
													initInfos := iinfoIdx[str(lp["INIT"])]
													if len(initInfos) > 0 {
														r.LoopINIT = strPtr(initInfos[0]["Name"])
													}
												}
											}
										}

										results = append(results, r)
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return results, nil
}

func queryQ2(db *Database) ([]CVRoleConstraintRow, error) {
	cvRole, err := loadTable(db, "CVRole")
	if err != nil {
		return nil, err
	}
	varRole, err := loadTable(db, "VariableRole")
	if err != nil {
		return nil, err
	}
	cvRef, err := loadTable(db, "ControllerVariableReference")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	vrIdx := indexBy(varRole, "RoleIdentifier")
	cvRefIdx := indexBy(cvRef, "ControllerVariableReferenceIdentifier")
	iiIdx := indexBy(iinfo, "Identifier")

	var results []CVRoleConstraintRow
	for _, cv := range cvRole {
		vrs := vrIdx[str(cv["CVRoleIdentifier"])]
		if len(vrs) == 0 {
			continue
		}
		for _, vr := range vrs {
			refs := cvRefIdx[str(vr["VariableReferenceIdentifier"])]
			if len(refs) == 0 {
				continue
			}
			for _, ref := range refs {
				iis := iiIdx[str(ref["ProcessVariableId"])]
				if len(iis) == 0 {
					continue
				}
				for _, ii := range iis {
					results = append(results, CVRoleConstraintRow{
						VariableName:        str(ii["Name"]),
						VariableDescription: str(ii["Description"]),
						RoleType:            str(vr["RoleType"]),
						IsLoLimitSpec:       toBool(cv["IsLoLimitSpec"]),
						IsHiLimitSpec:       toBool(cv["IsHiLimitSpec"]),
						IsSetPointSpec:      toBool(cv["IsSetPointSpec"]),
						MinTimeToLimit:      toFloat64(cv["MinTimeToLimit"]),
						RampRateImbalanceLo: toFloat64(cv["RampRateImbalanceLo"]),
						RampRateImbalanceHi: toFloat64(cv["RampRateImbalanceHi"]),
						RampImbalanceMethod: str(cv["RampImbalanceMethod"]),
					})
				}
			}
		}
	}
	return results, nil
}

func queryQ3(db *Database) ([]EconomicFunctionRow, error) {
	ef, err := loadTable(db, "EconomicFunction")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	iiIdx := indexBy(iinfo, "Identifier")

	var results []EconomicFunctionRow
	for _, e := range ef {
		iis := iiIdx[str(e["ControllerIdentifier"])]
		if len(iis) == 0 {
			continue
		}
		for _, ii := range iis {
			results = append(results, EconomicFunctionRow{
				ControllerName:   str(ii["Name"]),
				ObjectiveFormula: str(e["FormulaString"]),
				IsFormulaValid:   toBool(e["IsFormulaValid"]),
			})
		}
	}
	return results, nil
}

func queryQ4(db *Database) ([]VariableTransformRow, error) {
	vt, err := loadTable(db, "VariableTransform")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	iiIdx := indexBy(iinfo, "Identifier")

	var results []VariableTransformRow
	for _, v := range vt {
		iis := iiIdx[str(v["VariableIdentifier"])]
		if len(iis) == 0 {
			continue
		}
		for _, ii := range iis {
			results = append(results, VariableTransformRow{
				VariableName: str(ii["Name"]),
				Formula:      str(v["Formula"]),
				Min:          toFloat64(v["Min"]),
				Max:          toFloat64(v["Max"]),
				PointNumber:  toInt32(v["PointNumber"]),
			})
		}
	}
	return results, nil
}

func queryQ5(db *Database) ([]ModelMetadataRow, error) {
	models, err := loadTable(db, "Models")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	iiIdx := indexBy(iinfo, "Identifier")

	var results []ModelMetadataRow
	for _, m := range models {
		iis := iiIdx[str(m["ModelIdentifier"])]
		if len(iis) == 0 {
			continue
		}
		for _, ii := range iis {
			results = append(results, ModelMetadataRow{
				ModelName:                     str(ii["Name"]),
				ModelDescription:              str(ii["Description"]),
				ModelHorizonInSeconds:         toInt32(m["ModelHorizonInSeconds"]),
				PlotIntervalInSeconds:         toInt32(m["PlotIntervalInSeconds"]),
				LargestSettlingTimeInSeconds:  toInt32(m["LargestSettlingTimeInSeconds"]),
				SmallestSettlingTimeInSeconds: toInt32(m["SmallestSettlingTimeInSeconds"]),
			})
		}
	}
	return results, nil
}

func queryQ6(db *Database) ([]ExecutionSequenceRow, error) {
	es, err := loadTable(db, "ExecutionSequence")
	if err != nil {
		return nil, err
	}

	results := make([]ExecutionSequenceRow, 0, len(es))
	for _, e := range es {
		results = append(results, ExecutionSequenceRow{
			ExecutionSequenceIdentifier:     str(e["ExecutionSequenceIdentifier"]),
			IsDefault:                       toBool(e["IsDefault"]),
			ExecutionIntervalInMilliseconds: toInt32(e["ExecutionIntervalInMilliseconds"]),
		})
	}
	return results, nil
}

func queryQ7(db *Database) ([]UserParameterRow, error) {
	up, err := loadTable(db, "UserParameter")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	iiIdx := indexBy(iinfo, "Identifier")

	var results []UserParameterRow
	for _, u := range up {
		iis := iiIdx[str(u["UserParameterId"])]
		if len(iis) == 0 {
			continue
		}
		for _, ii := range iis {
			results = append(results, UserParameterRow{
				ParameterName:        str(ii["Name"]),
				ParameterDescription: str(ii["Description"]),
				EngineeringUnits:     str(u["EngineeringUnits"]),
				UserParameterType:    str(u["UserParameterType"]),
				IsOperatorEditable:   toBool(u["IsOperatorEditable"]),
				IsLocal:              toBool(u["IsLocal"]),
			})
		}
	}
	return results, nil
}

func queryQ8(db *Database) ([]LoopDetailRow, error) {
	loopTbl, err := loadTable(db, "Loop")
	if err != nil {
		return nil, err
	}
	iinfo, err := loadTable(db, "ItemInformation")
	if err != nil {
		return nil, err
	}

	iiIdx := indexBy(iinfo, "Identifier")
	loopIdx := indexBy(loopTbl, "LoopIdentifier")

	var results []LoopDetailRow
	for _, l := range loopTbl {
		loopId := str(l["LoopIdentifier"])
		iis := iiIdx[loopId]
		if len(iis) == 0 {
			continue
		}
		for _, lii := range iis {
			r := LoopDetailRow{
				LoopName:        str(lii["Name"]),
				LoopDescription: str(lii["Description"]),
				Ranking:         strPtr(l["Ranking"]),
				DCSSystem:       strPtr(l["DCSSystem"]),
				PIDAlgorithm:    strPtr(l["PIDAlgorithm"]),
				PIDForm:         strPtr(l["PIDForm"]),
				PIDEquation:     strPtr(l["PIDEquation"]),
				LoopStatus:      strPtr(l["LoopStatus"]),
				PVTrack:         boolPtr(l["PVTrack"]),
			}

			if pvInfos := iiIdx[str(l["PV"])]; len(pvInfos) > 0 {
				r.PV = strPtr(pvInfos[0]["Name"])
			}
			if spInfos := iiIdx[str(l["SP"])]; len(spInfos) > 0 {
				r.SP = strPtr(spInfos[0]["Name"])
			}
			if opInfos := iiIdx[str(l["OP"])]; len(opInfos) > 0 {
				r.OP = strPtr(opInfos[0]["Name"])
			}
			if modeInfos := iiIdx[str(l["MODE"])]; len(modeInfos) > 0 {
				r.MODE = strPtr(modeInfos[0]["Name"])
			}
			if initInfos := iiIdx[str(l["INIT"])]; len(initInfos) > 0 {
				r.INIT = strPtr(initInfos[0]["Name"])
			}

			slaveLoopId := str(l["SlaveLoop"])
			if slaveLoopId != "" {
				if sls := loopIdx[slaveLoopId]; len(sls) > 0 {
					if slaveIIs := iiIdx[str(sls[0]["LoopIdentifier"])]; len(slaveIIs) > 0 {
						r.SlaveLoopName = strPtr(slaveIIs[0]["Name"])
					}
				}
			}

			results = append(results, r)
		}
	}
	return results, nil
}
