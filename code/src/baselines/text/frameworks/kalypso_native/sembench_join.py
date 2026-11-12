"""Original text Join using native CartesianProduct and native SemFilter."""
from .sembench_filter import FilterDriver
from .join_inputs import prepare


class JoinDriver(FilterDriver):
    operator_kind='join'
    operator_name='CartesianProduct/SemFilter Join'
    prompt_parser_identity='all declared fields in author Join template; native Filter predicate; not LOTUS parser'

    def prepare_operator(self,spec,directory,operation):
        table,paths=prepare(self.cfg,spec,directory,operation)
        payload=dict(data_path=str(paths['left']),ops=[dict(op='join',args=dict(
            right_table=str(paths['right']),instruction=operation.instruction))],blocking=False)
        rows=[dict(row_id=row.row_id,source_position=row.source_position) for row in operation.records]
        return rows,payload

    def operator_relation(self,operation,invoke,decisions):
        from lotus.types import LMOutput
        from src.baselines.text.sembench_prepared_join import _answers
        from src.baselines.text.frameworks.lotus_native_join import make_lm
        lm=make_lm(operation,1);calls=[]
        def answer(current,messages,*args,**kwargs):
            if current is not lm or calls or len(messages)!=len(decisions):
                raise ValueError('Original Join relation changed its complete ordered pair set')
            calls.append(True)
            return LMOutput(outputs=['True' if row['decision'] else 'False' for row in decisions])
        # Only restore the original relation. All model work was native
        # Kalypso, with all declared fields; these SDK messages are not sent.
        with _answers(lm,answer):return invoke()
