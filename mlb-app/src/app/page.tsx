"use client"
import { useRouter } from "next/navigation"

export default function Home() {
  const router = useRouter()

  return (
    <div className="min-h-screen flex flex-col items-center pt-32 bg-black text-white">
      <div className="text-center mb-16">
        <h1 className="text-5xl font-mono font-light tracking-wide mb-2">[ONE OF ONE INTELLIGENCE]</h1>
      </div>

      <div className="flex flex-col md:flex-row gap-16 mb-16">
        {/* Model 1 - ALPHA */}
        <div
          className="flex flex-col items-center cursor-pointer transition-transform hover:scale-105"
          onClick={() => router.push("/alpha")}
        >
          <div className="bg-teal-900/20 p-6 rounded-xl border border-teal-800/50 hover:border-teal-600/70 transition-colors">
            <img src="/team-logos/monkeyking5.png" alt="Alpha Model" className="w-48 h-auto" />
          </div>
          <span className="mt-4 text-2xl font-normal text-teal-300">[ ALPHA ]</span>
        </div>

        {/* Model 2 - OMEGA */}
        <div
          className="flex flex-col items-center cursor-pointer transition-transform hover:scale-105"
          onClick={() => router.push("/omega")}
        >
          <div className="bg-purple-900/20 p-6 rounded-xl border border-purple-800/50 hover:border-purple-600/70 transition-colors">
            <img src="/team-logos/monkeyking4.png" alt="Omega Model" className="w-48 h-auto" />
          </div>
          <span className="mt-4 text-2xl font-normal text-purple-300">[ OMEGA ]</span>
        </div>
      </div>

      <div className="mt-auto mb-8 text-center text-gray-400 text-sm">
        <p>© 1 OF 1 INTELLIGENCE LLC</p>
      </div>
    </div>
  )
}
