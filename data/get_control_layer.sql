-- get_control_layer.sql
-- Extracts the full control-layer model definition from a QuantumLeap / SMOC
-- SQLite database (converted from .sdf).
--
-- Query 1: Control layer transfer-function matrix with full metadata
-- Queries 2-8: Supplementary application data (CV constraints, economics,
--              transforms, models, execution config, user params, loops)

--------------------------------------------------------------------------------
-- 1. CONTROL LAYER (enhanced)
--------------------------------------------------------------------------------
SELECT
    iinfo.Name                          AS AppName,
    rb.BlockType                        AS BlockType,
    blks.Name                           AS BlockName,
    blks.Description                    AS BlockDescription,
    -- Block-level model settings
    blks.ModelHorizonInSeconds          AS BlockModelHorizon,
    blks.LargestSettlingTimeInSeconds   AS BlockLargestTSS,
    blks.SmallestSettlingTimeInSeconds  AS BlockSmallestTSS,
    -- BLC configuration
    blc.Representation                  AS BLC_configuration,
    blc.IntendedMV                      AS BLC_IntendedMV,
    blc.IntendedModelLoopType           AS BLC_LoopType,
    blc.Status                          AS BLC_Status,
    -- Loop PV / SP / OP
    lpv.Name                            AS PV,
    lsp.Name                            AS SP,
    lop.Name                            AS OP,
    lmode.Name                          AS LoopMODE,
    linit.Name                          AS LoopINIT,
    -- Loop tuning metadata
    l.Ranking                           AS LoopRanking,
    l.DCSSystem                         AS LoopDCSSystem,
    l.PIDAlgorithm                      AS LoopPIDAlgorithm,
    l.PIDForm                           AS LoopPIDForm,
    l.PIDEquation                       AS LoopPIDEquation,
    l.PVTrack                           AS LoopPVTrack,
    -- NOTE: Some older DBs have blc.BLCFeedbackVariableIdentifier -- omitted here
    -- because converted-from-SDF databases may lack that column.
    -- SISO relation flags
    sisor.IsDisturbance                 AS IsDisturbance,
    -- SISO element metadata
    sisoe.Name                          AS SisoElementName,
    sisoe.SisoElementType               AS SisoElementType,
    -- Input variable
    inputvrol.RoleType                  AS InputProcessVariableRoleType,
    inputiinfo.Name                     AS InputProcessVariableName,
    inputpv.EngineeringUnits            AS InputProcessVariableEU,
    inputiinfo.Description              AS InputProcessVariableDescription,
    inputpv.NormalMove                  AS InputNormalMove,
    inputpv.MeasurementType             AS InputMeasurementType,
    -- Output variable
    outputvrol.RoleType                 AS OutputProcessVariableRoleType,
    outputiinfo.Name                    AS OutputProcessVariableName,
    outputpv.EngineeringUnits           AS OutputProcessVariableEU,
    outputiinfo.Description             AS OutputProcessVariableDescription,
    outputpv.NormalMove                 AS OutputNormalMove,
    outputpv.MeasurementType            AS OutputMeasurementType,
    -- Transfer-function parameters
    pare.TransferFunction,
    pare.IsActive                       AS ElementIsActive,
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
    pare.SettlingTimeInSeconds           AS tss,
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
    -- Input variable chain
    INNER JOIN ProcessVariables       AS inputpv    ON sisor.InputProcessVariableIdentifier  = inputpv.ProcessVariableIdentifier
    INNER JOIN ItemInformation        AS inputiinfo ON inputpv.ProcessVariableIdentifier     = inputiinfo.Identifier
    INNER JOIN ControllerVariableReference AS inputcvref ON inputiinfo.Identifier = inputcvref.ProcessVariableId
    INNER JOIN VariableRole           AS inputvrol  ON inputcvref.ControllerVariableReferenceIdentifier = inputvrol.VariableReferenceIdentifier
                                                       AND inputvrol.RoleType IN ('MV', 'DV', 'POV')
    -- Output variable chain
    INNER JOIN ProcessVariables       AS outputpv    ON sisor.OutputProcessVariableIdentifier = outputpv.ProcessVariableIdentifier
    INNER JOIN ItemInformation        AS outputiinfo ON outputpv.ProcessVariableIdentifier    = outputiinfo.Identifier
    INNER JOIN ControllerVariableReference AS outputcvref ON outputiinfo.Identifier = outputcvref.ProcessVariableId
    INNER JOIN VariableRole           AS outputvrol  ON outputcvref.ControllerVariableReferenceIdentifier = outputvrol.VariableReferenceIdentifier
                                                        AND outputvrol.RoleType IN ('CV', 'POV', 'DV')
    -- Optional BLC / Loop joins
    LEFT JOIN BlcModel          AS blc    ON blks.BlockIdentifier = blc.BlcModelBlockId
    LEFT JOIN Loop              AS l      ON blc.LoopIdentifier   = l.LoopIdentifier
    LEFT JOIN ItemInformation   AS lpv    ON l.PV   = lpv.Identifier
    LEFT JOIN ItemInformation   AS lsp    ON l.SP   = lsp.Identifier
    LEFT JOIN ItemInformation   AS lop    ON l.OP   = lop.Identifier
    LEFT JOIN ItemInformation   AS lmode  ON l.MODE = lmode.Identifier
    LEFT JOIN ItemInformation   AS linit  ON l.INIT = linit.Identifier
    -- LEFT JOIN ItemInformation AS blcfb ON blc.BLCFeedbackVariableIdentifier = blcfb.Identifier
    --   ^ Uncomment if your DB has the BLCFeedbackVariableIdentifier column
;

--------------------------------------------------------------------------------
-- 2. CV ROLE CONSTRAINTS
--    Limit specs, setpoint specs, ramp-rate imbalance settings per CV.
--------------------------------------------------------------------------------
SELECT
    ii.Name                     AS VariableName,
    ii.Description              AS VariableDescription,
    vr.RoleType,
    cv.IsLoLimitSpec,
    cv.IsHiLimitSpec,
    cv.IsSetPointSpec,
    cv.MinTimeToLimit,
    cv.RampRateImbalanceLo,
    cv.RampRateImbalanceHi,
    cv.RampImbalanceMethod
FROM CVRole cv
    INNER JOIN VariableRole vr
        ON cv.CVRoleIdentifier = vr.RoleIdentifier
    INNER JOIN ControllerVariableReference cvref
        ON vr.VariableReferenceIdentifier = cvref.ControllerVariableReferenceIdentifier
    INNER JOIN ItemInformation ii
        ON cvref.ProcessVariableId = ii.Identifier
;

--------------------------------------------------------------------------------
-- 3. ECONOMIC FUNCTIONS
--    Objective formulas used by the optimizer.
--------------------------------------------------------------------------------
SELECT
    ii.Name                     AS ControllerName,
    ef.FormulaString            AS ObjectiveFormula,
    ef.IsFormulaValid
FROM EconomicFunction ef
    INNER JOIN ItemInformation ii
        ON ef.ControllerIdentifier = ii.Identifier
;

--------------------------------------------------------------------------------
-- 4. VARIABLE TRANSFORMS
--    Formula-based variable transformations (e.g. linearization).
--------------------------------------------------------------------------------
SELECT
    ii.Name                     AS VariableName,
    vt.Formula,
    vt.Min,
    vt.Max,
    vt.PointNumber
FROM VariableTransform vt
    INNER JOIN ItemInformation ii
        ON vt.VariableIdentifier = ii.Identifier
;

--------------------------------------------------------------------------------
-- 5. MODEL METADATA
--    Top-level model definitions (horizon, plot interval).
--------------------------------------------------------------------------------
SELECT
    ii.Name                     AS ModelName,
    ii.Description              AS ModelDescription,
    m.ModelHorizonInSeconds,
    m.PlotIntervalInSeconds,
    m.LargestSettlingTimeInSeconds,
    m.SmallestSettlingTimeInSeconds
FROM Models m
    INNER JOIN ItemInformation ii
        ON m.ModelIdentifier = ii.Identifier
;

--------------------------------------------------------------------------------
-- 6. EXECUTION SEQUENCE
--    Controller execution interval in milliseconds.
--------------------------------------------------------------------------------
SELECT
    es.ExecutionSequenceIdentifier,
    es.IsDefault,
    es.ExecutionIntervalInMilliseconds
FROM ExecutionSequence es
;

--------------------------------------------------------------------------------
-- 7. USER PARAMETERS
--    Custom operator-editable parameters (costs, prices, etc.).
--------------------------------------------------------------------------------
SELECT
    ii.Name                     AS ParameterName,
    ii.Description              AS ParameterDescription,
    up.EngineeringUnits,
    up.UserParameterType,
    up.IsOperatorEditable,
    up.IsLocal
FROM UserParameter up
    INNER JOIN ItemInformation ii
        ON up.UserParameterId = ii.Identifier
;

--------------------------------------------------------------------------------
-- 8. LOOP DETAILS (standalone)
--    Full loop inventory independent of BLC join path.
--------------------------------------------------------------------------------
SELECT
    lii.Name                    AS LoopName,
    lii.Description             AS LoopDescription,
    lpv.Name                    AS PV,
    lsp.Name                    AS SP,
    lop.Name                    AS OP,
    lmode.Name                  AS MODE,
    linit.Name                  AS INIT,
    l.Ranking,
    l.DCSSystem,
    l.PIDAlgorithm,
    l.PIDForm,
    l.PIDEquation,
    l.LoopStatus,
    l.PVTrack,
    slave_ii.Name               AS SlaveLoopName
FROM Loop l
    INNER JOIN ItemInformation  AS lii   ON l.LoopIdentifier = lii.Identifier
    LEFT JOIN  ItemInformation  AS lpv   ON l.PV        = lpv.Identifier
    LEFT JOIN  ItemInformation  AS lsp   ON l.SP        = lsp.Identifier
    LEFT JOIN  ItemInformation  AS lop   ON l.OP        = lop.Identifier
    LEFT JOIN  ItemInformation  AS lmode ON l.MODE      = lmode.Identifier
    LEFT JOIN  ItemInformation  AS linit ON l.INIT      = linit.Identifier
    LEFT JOIN  Loop             AS sl    ON l.SlaveLoop  = sl.LoopIdentifier
    LEFT JOIN  ItemInformation  AS slave_ii ON sl.LoopIdentifier = slave_ii.Identifier
;
