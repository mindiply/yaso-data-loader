import {NamedParameter} from 'yaso/lib/query/SQLExpression';
import {prm} from 'yaso';

export type ParametrizedObj<R extends Record<keyof R, unknown>> = {
  [K in keyof R]: NamedParameter;
};

export function parametrizeRecordData<R extends Record<keyof R, unknown>>(
  valuesRecord: Partial<R>
): Partial<ParametrizedObj<R>> {
  const parametrizedObj: Partial<ParametrizedObj<R>> = {};
  for (const fieldName in valuesRecord) {
    parametrizedObj[fieldName] = prm(fieldName);
  }
  return parametrizedObj as ParametrizedObj<R>;
}
