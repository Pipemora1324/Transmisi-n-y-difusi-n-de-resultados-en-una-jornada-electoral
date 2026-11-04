export function verificationKeys(prefix) {
  return {
    queue: `${prefix}queue:verify`, heldPrefix: `${prefix}held:`, active: `${prefix}verify:active`, claims: `${prefix}claims`,
    lockPrefix: `${prefix}lock:acta:`, events: `${prefix}ch:audit`, audit: `${prefix}verify:audit`,
    presence: `${prefix}verify:presence`, people: `${prefix}verify:people`,
  };
}
