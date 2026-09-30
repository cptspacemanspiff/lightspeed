declare module "workerd" {
  // The package is CommonJS with a `default` export, so a default import
  // yields the path or the exports object depending on the loader.
  const exported: string | { default: string };
  export default exported;
}
